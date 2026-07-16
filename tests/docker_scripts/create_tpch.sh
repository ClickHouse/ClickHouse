#!/bin/bash
set -euo pipefail

# Creates TPC-H SF1 tables in tpch database using INSERT ... SELECT FROM s3().

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INIT_SQL="$REPO_ROOT/tests/benchmarks/tpc-h/init.sql"
S3_BASE="https://clickhouse-datasets.s3.amazonaws.com/h/1"

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tpch"

awk '
/^--/ { next }
/^[[:space:]]*$/ { next }
/^CREATE TABLE / {
    sub(/CREATE TABLE /, "CREATE TABLE tpch.")
}
{ print }
' "$INIT_SQL" | clickhouse-client -m

TABLES=(nation region part supplier partsupp customer orders lineitem)

for table in "${TABLES[@]}"; do
    clickhouse-client --query "INSERT INTO tpch.${table} SELECT * FROM s3('${S3_BASE}/${table}.tbl', NOSIGN, CSV) SETTINGS format_csv_delimiter='|', input_format_defaults_for_omitted_fields=1, input_format_csv_empty_as_default=1"
done
