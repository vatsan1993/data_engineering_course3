import sqlite3
import pandas as pd

# Connect to the SQLite3 service
conn = sqlite3.connect('STAFF.db')

# Define table parameters
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read the CSV data
file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Load the CSV to the database
df.to_sql(table_name, conn, if_exists = 'replace', index = False)
print('Table is ready')

# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Define data to be appended
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

# Append data to the table
data_append.to_sql(table_name, conn, if_exists = 'append', index = False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


data = pd.read_csv('Departments.csv', header = None)
data.columns = ['DEPT_ID', 'DEPT_NAME', 'MANAGER_ID', 'LOC_ID']

data.to_sql('DEPARTMENT', conn, if_exists = 'replace', index = False)

# # Define data to be appended
data_dict = {'DEPT_ID' : [9],
            'DEPT_NAME' : ['Quality Assurance'],
            'MANAGER_ID' : [30010],
            'LOC_ID' : ['L0010'],
}
data_append = pd.DataFrame(data_dict)

# # Append data to the table
data_append.to_sql('DEPARTMENT', conn, if_exists = 'append', index = False)

print(pd.read_sql("SELECT * from DEPARTMENT",conn))
print(pd.read_sql("SELECT DEPT_NAME from DEPARTMENT",conn))
print(pd.read_sql("SELECT count(*) from DEPARTMENT",conn))
# Close the connection
conn.close()
