#!/bin/bash

# This script tests the MySQL compatibility of the SHOW COLUMNS command in ClickHouse
USER="default"
PASSWORD=""
HOST="127.0.0.1"
PORT=9004

# First run the clickhouse test to create the ClickHouse Tables

echo "Drop tables if they exist"
${CLICKHOUSE_LOCAL} --query "DROP TABLE IF EXISTS tab"
${CLICKHOUSE_LOCAL} --query "DROP TABLE IF EXISTS database_123456789abcde"
${CLICKHOUSE_LOCAL} --query "DROP TABLE IF EXISTS database_123456789abcde.tab"

echo "Create tab table "
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE tab
    (
        uint64 UInt64,
        int32 Nullable(Int32),
        float32 Float32,
        float64 Float64,
        decimal_value Decimal(10, 2),
        boolean_value UInt8, -- Use 0 for false, 1 for true
        string_value String,
        fixed_string_value FixedString(10),
        date_value Date,
        date32_value Date32,
        datetime_value DateTime,
        datetime64_value DateTime64(3),
        json_value String, -- Store JSON as a string
        uuid_value UUID,
        enum_value Enum8('apple' = 1, 'banana' = 2, 'orange' = 3),
        low_cardinality LowCardinality(String),
        array_value Array(Int32),
        map_value Map(String, Int32),
        tuple_value Tuple(Int32, String),
        nullable_value Nullable(Int32),
        ipv4_value IPv4,
        ipv6_value IPv6,
        nested Nested
        (
            nested_int Int32,
            nested_string String
        )
    ) ENGINE = MergeTree
    ORDER BY uint64;
    "


echo "Create pseudo-random database name"
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE database_123456789abcde;"

echo "Create tab duplicate table"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE database_123456789abcde.tab
    (
        uint64 UInt64,
        int32 Nullable(Int32),
        float32 Float32,
        float64 Float64,
        decimal_value Decimal(10, 2),
        boolean_value UInt8, -- Use 0 for false, 1 for true
        string_value String,
        fixed_string_value FixedString(10),
        date_value Date,
        date32_value Date32,
        datetime_value DateTime,
        datetime64_value DateTime64(3),
        json_value String, -- Store JSON as a string
        uuid_value UUID,
        enum_value Enum8('apple' = 1, 'banana' = 2, 'orange' = 3),
        low_cardinality LowCardinality(String),
        array_value Array(Int32),
        map_value Map(String, Int32),
        tuple_value Tuple(Int32, String),
        nullable_value Nullable(Int32),
        ipv4_value IPv4,
        ipv6_value IPv6,
        nested Nested
        (
            nested_int Int32,
            nested_string String
        )
    ) ENGINE = MergeTree
    ORDER BY uint64;
    "

# Write sql to temp file 
TEMP_FILE=$(mktemp)

cat <<EOT > $TEMP_FILE
SHOW COLUMNS FROM tab;
SHOW EXTENDED COLUMNS FROM tab;
SHOW FULL COLUMNS FROM tab;
SHOW COLUMNS FROM tab LIKE '%int%';
SHOW COLUMNS FROM tab NOT LIKE '%int%';
SHOW COLUMNS FROM tab ILIKE '%INT%';
SHOW COLUMNS FROM tab NOT ILIKE '%INT%';
SHOW COLUMNS FROM tab WHERE field LIKE '%int%';
SHOW COLUMNS FROM tab LIMIT 1;
SHOW COLUMNS FROM tab;
SHOW COLUMNS FROM tab FROM database_123456789abcde;
SHOW COLUMNS FROM database_123456789abcde.tab;
DROP DATABASE database_123456789abcde;
DROP TABLE tab;
EOT

# Now run the MySQL test script on the ClickHouse DB
echo "Run MySQL test"
mysql --user="$USER" --password="$PASSWORD" --host="$HOST" --port="$PORT" < $TEMP_FILE

# Clean up the temp file
rm $TEMP_FILE

