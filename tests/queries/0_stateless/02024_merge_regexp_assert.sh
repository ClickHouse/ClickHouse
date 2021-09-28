#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function run_with_retry
{
    query=$1
    error=$2

    for _ in {1..200}; do
        ${CLICKHOUSE_CLIENT} -q "$query" 2>&1 | grep -a -o -m1 "$error" && break
    done
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t;"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (b UInt8) ENGINE = Memory;"

# Run with retries, because some databases may be dropped concurrently and we will receive wrong error code
run_with_retry "SELECT a FROM merge(REGEXP('.'), '^t$');" "UNKNOWN_IDENTIFIER"
run_with_retry "SELECT a FROM merge(REGEXP('\0'), '^t$');" "UNKNOWN_IDENTIFIER"
run_with_retry "SELECT a FROM merge(REGEXP('\0a'), '^t$');" "UNKNOWN_IDENTIFIER"
run_with_retry "SELECT a FROM merge(REGEXP('\0a'), '^$');" "BAD_ARGUMENTS"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t;"
