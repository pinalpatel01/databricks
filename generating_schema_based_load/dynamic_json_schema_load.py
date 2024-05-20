# Databricks notebook source
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Initialize Spark session
spark = SparkSession.builder.appName("Load JSON Data with Schema").getOrCreate()

# Load schema.json file
schema_file_path = "dbfs:/FileStore/pinal/results_schema.json"

# Read schema.json from DBFS
schema_json_df = spark.read.text(schema_file_path)
schema_json_str = ''.join([row['value'] for row in schema_json_df.collect()])

# Print the schema JSON string to debug
print("Schema JSON String:", schema_json_str)


# COMMAND ----------

# Convert the JSON schema to a StructType
schema = StructType.fromJson(json.loads(schema_json_str))

# Print the schema to debug
print("Schema StructType:", schema)



# COMMAND ----------

# Load JSON data using the defined schema
json_file_path = "dbfs:/FileStore/pinal/results.json"

# Load JSON data using the schema
try:
    df = spark.read.schema(schema).json(json_file_path)
    # Show the DataFrame
    df.show()
except Exception as e:
    print("Error loading JSON data with schema:", e)
    # Load JSON data without schema to debug the data
    df_raw = spark.read.json(json_file_path)
    df_raw.show()
    df_raw.printSchema()
