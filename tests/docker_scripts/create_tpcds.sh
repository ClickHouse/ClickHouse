#!/bin/bash
set -euo pipefail

# Creates TPC-DS SF1 tables in tpcds database by transforming tests/benchmarks/tpc-ds/init.sql to use S3 web disk.

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INIT_SQL="$REPO_ROOT/tests/benchmarks/tpc-ds/init.sql"
S3_BASE="https://tpc-ds-sf1.s3.amazonaws.com"

# The stress harness exports `NO_AST_FUZZER` (see `stress_tests.lib`) to keep the
# server-side AST fuzzer off our own maintenance queries; it is empty when this script
# runs outside of that harness.

clickhouse-client ${NO_AST_FUZZER:-} --query "CREATE DATABASE IF NOT EXISTS tpcds"

awk -v s3="$S3_BASE" '
/^---/ { next }
/^[[:space:]]*$/ { next }
/^CREATE TABLE / {
    name = $3
    gsub(/\(.*/, "", name)
    current_table = name
    sub(/CREATE TABLE /, "CREATE TABLE tpcds.")
}
/^);/ {
    print ")"
    print "ENGINE = MergeTree"
    print "SETTINGS"
    print "    table_disk = 1,"
    printf "    disk = disk(type = web, endpoint = \047%s/%s/\047);\n\n", s3, current_table
    next
}
{ print }
' "$INIT_SQL" | clickhouse-client ${NO_AST_FUZZER:-} -m --data_type_default_nullable=1
