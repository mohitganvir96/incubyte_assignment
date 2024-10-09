import pandas as pd
import mysql.connector
import argparse
from datetime import datetime

"""Summary: This script extracts customer data from a text file, transforms it by 
calculating age and days since the last consultation, 
and loads it into a MySQL database while ensuring no duplicates are created."""

# Argparse to parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('user')  # MySQL username
parser.add_argument('pwd')   # MySQL password
args = parser.parse_args()

user = args.user
pwd = args.pwd
file_path = r'C:\Users\Lenovo\Desktop\incubyte\customers.txt'  # Path to input data file

# Function to calculate age from date of birth
def calculate_age(dob):
    if pd.isnull(dob):
        return None
    today = datetime.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

# Function to calculate days since the last consultation
def calculate_days_since_last_consultation(last_consulted_date):
    if pd.isnull(last_consulted_date):
        return None
    last_consulted = pd.to_datetime(last_consulted_date, format='%Y%m%d', errors='coerce')  # Convert to datetime
    return (datetime.today() - last_consulted).days

# Function to establish a connection to the MySQL database
def connect_db(username, password):
    conn = mysql.connector.connect(
        host="localhost",
        user=username,
        password=password,
        database="hospital_db"  
    )
    return conn

# Function to read customer data from a text file
def get_customer_data(file_path):
    data = pd.read_csv(file_path, sep='|', header=None, skiprows=1)
    data.columns = ['record_type', 'customer_name', 'customer_id', 'open_date', 
                    'last_consulted_date', 'vaccination_id', 'doctor_name', 
                    'state', 'country', 'dob', 'is_active']  # Set column names
    return data

# Function to create a table for the specified country if it doesn't exist
def create_country_table(country, conn):
    cursor = conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS Table_{country} (
        customer_name VARCHAR(255) NOT NULL,
        customer_id VARCHAR(18) NOT NULL,
        open_date DATE NOT NULL,
        last_consulted_date DATE,
        vaccination_id CHAR(5),
        doctor_name CHAR(255),
        state CHAR(5),
        country CHAR(5),
        dob DATE,
        is_active CHAR(1),
        age INT,
        days_since_last_consultation INT
    )
    """
    cursor.execute(create_table_query) 
    conn.commit()
    print(f"Table Table_{country} created or already exists.")
    cursor.close()

# Function to insert data into the specified country table
def insert_into_country_table(df, country_table, conn):
    cursor = conn.cursor()
    
    # Truncate the table before inserting new data to avoid duplicates
    truncate_query = f"TRUNCATE TABLE {country_table}"
    cursor.execute(truncate_query)

    # Insert new records
    insert_query = f"""
    INSERT INTO {country_table} 
    (customer_name, customer_id, open_date, last_consulted_date, vaccination_id, 
    doctor_name, state, country, dob, is_active, age, days_since_last_consultation)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for _, row in df.iterrows():  # Iterate over each row in the DataFrame
        cursor.execute(insert_query, (
            row['customer_name'], row['customer_id'], row['open_date'], 
            row['last_consulted_date'], row['vaccination_id'], row['doctor_name'], 
            row['state'], row['country'], row['dob'], row['is_active'], 
            row['age'], row['days_since_last_consultation']
        ))

    conn.commit()  
    print(f"Data inserted into {country_table}")
    cursor.close()

# Main function to run the ETL process
def run_etl():
    data = get_customer_data(file_path)  
    
    data = data[data['record_type'] == 'D']  
    data = data.drop(columns=['record_type'])  
    
    # Convert date columns to datetime format
    data['open_date'] = pd.to_datetime(data['open_date'].astype(str), format='%Y%m%d', errors='coerce')
    data['last_consulted_date'] = pd.to_datetime(data['last_consulted_date'].astype(str), format='%Y%m%d', errors='coerce')
    
    # Convert dob from DDMMYYYY to datetime
    data['dob'] = pd.to_datetime(data['dob'], format='%d%m%Y', errors='coerce')
    
    # Fill empty doctor_name with "no_data_provided"
    data['doctor_name'] = data['doctor_name'].fillna("no_data_provided")

    # Calculate age and days since last consultation
    data['age'] = data['dob'].apply(calculate_age)
    data['days_since_last_consultation'] = data['last_consulted_date'].apply(calculate_days_since_last_consultation)

    # Sort data by 'customer_id' and 'last_consulted_date'
    data = data.sort_values(by=['customer_id', 'last_consulted_date'], ascending=[True, False])

    # Group by 'customer_id' and keep the latest record based on 'last_consulted_date'
    data = data.loc[data.groupby('customer_id')['last_consulted_date'].idxmax()]

    # Remove timestamps from date columns before saving to Excel
    data['open_date'] = data['open_date'].dt.date
    data['last_consulted_date'] = data['last_consulted_date'].dt.date
    data['dob'] = data['dob'].dt.date

   
    data.to_excel(r"C:\Users\Lenovo\Desktop\incubyte\transformed_customer_data.xlsx", index=False)
    print("Transformed data saved to 'transformed_customer_data.xlsx'")

    conn = connect_db(user, pwd)  

    # Loop through unique countries in the data
    unique_countries = data['country'].unique()  # Get unique country values
    for country in unique_countries:
        country_table = f'Table_{country}'  
        create_country_table(country, conn)  # Create the country table if it doesn't exist
        
        country_data = data[data['country'] == country]  
        insert_into_country_table(country_data, country_table, conn)  
    
    conn.close()  

if __name__ == "__main__":
    run_etl()  
