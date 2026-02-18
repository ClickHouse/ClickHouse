#!/usr/bin/env bash

# Test for table name hints: ensure we don't suggest the exact same name from the same database,
# and that cross-database hints include the database name.
# https://github.com/ClickHouse/ClickHouse/issues/93101

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
other_db="${CLICKHOUSE_DATABASE}_hint"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE IF NOT EXISTS ${other_db}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${other_db}.my_table (x UInt64) ENGINE = MergeTree ORDER BY x"

# Exact same name exists only in another database - hint should point there, not suggest the same name in the same database.
${CLICKHOUSE_CLIENT} -q "DROP TABLE my_table" 2>&1 | grep 'Received from' | grep -oP 'Maybe you meant \S+' | sed "s/${other_db}/<other_db>/g"

# Similar but not identical name - should suggest the correct name from the other database.
${CLICKHOUSE_CLIENT} -q "DROP TABLE my_tabl" 2>&1 | grep 'Received from' | grep -oP 'Maybe you meant \S+' | sed "s/${other_db}/<other_db>/g"

# No similar name anywhere - no hint.
${CLICKHOUSE_CLIENT} -q "DROP TABLE zzz_completely_unrelated_name_12345" 2>&1 | grep 'Received from' | grep -c 'Maybe you meant' || true

# Similar name in the same database - hint should include the database prefix.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.my_other_table (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "DROP TABLE my_other_tabl" 2>&1 | grep 'Received from' | grep -oP 'Maybe you meant \S+' | sed "s/${db}/<db>/g" | sed "s/${other_db}/<other_db>/g"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.my_other_table"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${other_db}.my_table"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${other_db}"
