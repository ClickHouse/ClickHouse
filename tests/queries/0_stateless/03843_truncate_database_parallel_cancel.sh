#!/usr/bin/env bash
# Regression test: TRUNCATE DATABASE db TABLES LIKE pattern must pre-cancel background
# merges for all matching tables simultaneously before entering the thread pool.
#

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_trunc_parallel"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB};
    CREATE TABLE ${DB}.t1 (n UInt64) ENGINE = MergeTree() ORDER BY n;
    CREATE TABLE ${DB}.t2 (n UInt64) ENGINE = MergeTree() ORDER BY n;
    CREATE TABLE ${DB}.t3 (n UInt64) ENGINE = MergeTree() ORDER BY n;
    CREATE TABLE ${DB}.t4 (n UInt64) ENGINE = MergeTree() ORDER BY n;
    CREATE TABLE ${DB}.skip_me (n UInt64) ENGINE = MergeTree() ORDER BY n;
"

# Insert several parts into each table so background merges are likely to start.
for i in $(seq 1 8); do
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.t1 SELECT number FROM numbers(10000)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.t2 SELECT number FROM numbers(10000)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.t3 SELECT number FROM numbers(10000)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.t4 SELECT number FROM numbers(10000)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.skip_me SELECT number FROM numbers(10000)"
done

# TRUNCATE only the t* tables, leaving skip_me intact.
# This must complete promptly regardless of how many background merges are running.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLES FROM ${DB} LIKE 't%'"

# All matched tables must be empty.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.t1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.t2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.t3"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}.t4"

# The unmatched table must still have its data.
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM ${DB}.skip_me"
