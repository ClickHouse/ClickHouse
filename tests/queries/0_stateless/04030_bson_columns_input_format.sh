#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Base case for auto case
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String, NaMe String)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='auto';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_names.bson' FORMAT BSONEachRow;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Test ambiguity for automatic column name matching
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, iD Int, name String, NAME String)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='auto';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_names.bson' FORMAT BSONEachRow; -- { clientError 117 }"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Base case for match case
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String, NaMe String)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='match_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_names.bson' FORMAT BSONEachRow;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Base case for ignore case
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (ID Int, name String)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='ignore_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_names_no_duplicates.bson' FORMAT BSONEachRow;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Test ambiguity for ignore case column name matching
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String, NAME String)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='ignore_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_names.bson' FORMAT BSONEachRow; -- { clientError 117 }"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Test ambiguity when two input columns map to the same table column (auto case match)
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='auto';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_duplicated_names.bson' FORMAT BSONEachRow; -- { clientError 117 }"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

# Test ambiguity when two input columns map to the same table column (ignore case match)
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int)"
$CLICKHOUSE_CLIENT -q "SET input_format_column_name_matching_mode='ignore_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_bson/bson_with_duplicated_names.bson' FORMAT BSONEachRow; -- { clientError 117 }"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"