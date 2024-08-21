#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This script tests the MySQL compatibility of the SHOW COLUMNS command in ClickHouse
USER="default"
PASSWORD=""
HOST="127.0.0.1"
PORT=9004

# First run the clickhouse test to create the ClickHouse Tables

echo "Drop tables if they exist"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tab"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS database_123456789abcdef"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS database_123456789abcdef.tab"

echo "Create tab table "
${CLICKHOUSE_CLIENT} -n -q "
    SET allow_suspicious_low_cardinality_types=1;
    SET allow_experimental_object_type=1;
    CREATE TABLE tab
    (
        uint8 UInt8,
        uint16 UInt16,
        uint32 UInt32,
        uint64 UInt64,
        uint128 UInt128,
        uint256 UInt256,
        int8 Int8,
        int16 Int16,
        int32 Int32,
        int64 Int64,
        int128 Int128,
        int256 Int256,
        nint32 Nullable(Int32),
        float32 Float32,
        float64 Float64,
        decimal_value Decimal(10, 2),
        boolean_value UInt8,
        string_value String,
        fixed_string_value FixedString(10),
        date_value Date,
        date32_value Date32,
        datetime_value DateTime,
        datetime64_value DateTime64(3),
        json_value JSON,
        uuid_value UUID,
        enum_value Enum8('apple' = 1, 'banana' = 2, 'orange' = 3),
        low_cardinality LowCardinality(String),
        low_cardinality_date LowCardinality(DateTime),
        aggregate_function AggregateFunction(sum, Int32),
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
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE database_123456789abcdef;"

echo "Create tab duplicate table"
${CLICKHOUSE_CLIENT} -n -q "
    SET allow_suspicious_low_cardinality_types=1;
    SET allow_experimental_object_type =1;
    CREATE TABLE database_123456789abcdef.tab
    (
        uint8 UInt8,
        uint16 UInt16,
        uint32 UInt32,
        uint64 UInt64,
        uint128 UInt128,
        uint256 UInt256,
        int8 Int8,
        int16 Int16,
        int32 Int32,
        int64 Int64,
        int128 Int128,
        int256 Int256,
        nint32 Nullable(Int32),
        float32 Float32,
        float64 Float64,
        decimal_value Decimal(10, 2),
        boolean_value UInt8,
        string_value String,
        fixed_string_value FixedString(10),
        date_value Date,
        date32_value Date32,
        datetime_value DateTime,
        datetime64_value DateTime64(3),
        json_value JSON, 
        uuid_value UUID,
        enum_value Enum8('apple' = 1, 'banana' = 2, 'orange' = 3),
        low_cardinality LowCardinality(String),
        low_cardinality_date LowCardinality(DateTime),
        aggregate_function AggregateFunction(sum, Int32),
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
SET use_mysql_types_in_show_columns=1;
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
SHOW COLUMNS FROM tab FROM database_123456789abcdef;
SHOW COLUMNS FROM database_123456789abcdef.tab;
DROP DATABASE database_123456789abcdef;
DROP TABLE tab;
EOT

# Now run the MySQL test script on the ClickHouse DB
echo "Run MySQL test"
MYSQL_PWD=$PASSWORD ${MYSQL_CLIENT} --user="$USER" --host="$HOST" --port="$PORT" < $TEMP_FILE

# Clean up the temp file
rm $TEMP_FILE

