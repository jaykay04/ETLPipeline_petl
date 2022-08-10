import os
import sys
import petl 
import pymssql 
import configparser 
import requests
import datetime
import json
import decimal
import openpyxl


# Get data from configuration file
config = configparser.ConfigParser()
try:
    config.read("ETLDemo.ini")
except Exception as e:
    print("Could not read configuration file", e)
    sys.exit()


# Read Settings from Configuration file
startDate = config["CONFIG"]["startDate"]
url = config["CONFIG"]["url"]
destServer = config["CONFIG"]["server"]
destDatabase = config["CONFIG"]["database"]

# Request Data from url
try:
    BOCResponse = requests.get(url + startDate)
except Exception as e :
    print("Could not make request", e)
    sys.exit()

#  Initialize list of lists for data storage
BOCDates = []
BOCRates = []

# Check respoonse status and process BOc JSON obect
if BOCResponse.status_code == 200:
    BOCRaw = json.loads(BOCResponse.text)

    # Extract observation data into column arrays
    for row in BOCRaw["observations"]:
        BOCDates.append(datetime.datetime.strptime(row['d'],"%Y-%m-%d"))
        BOCRates.append(decimal.Decimal(row["FXUSDCAD"]["v"]))

    # Create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates], header = ["date","rate"])
    
    # Load Expense data
    try:
        expenses = petl.io.xlsx.fromxlsx("Expenses.xlsx", sheet = "Github")
    except Exception as e:
        print("Could not open Expenses data", e)
        sys.exit()

    # Join the two tables
    expenses = petl.outerjoin(exchangeRates, expenses, key = "date")
    
    # Fill down missing value
    expenses = petl.filldown(expenses, "rate")
    
    # Remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)
    
    # Add the CAD Column
    expenses = petl.addfield(expenses, "CAD", lambda rec: decimal.Decimal(rec.USD) * rec.rate)
    
    # Initialize Database Connection
    try:
        dbConnection = pymssql.connect(server = destServer, database = destDatabase)
    except Exception as e:
        print("Could not Connect to database", e)
        sys.exit()

    # Populate data to the expenses table in MSsql
    try:
        petl.io.todb(expenses, dbConnection,"Expenses")
    except Exception as e:
        print("Could not write to database", e)
        sys.exit()
