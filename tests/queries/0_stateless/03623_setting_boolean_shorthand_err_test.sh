#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test shorthand syntax with boolean works
${CLICKHOUSE_CLIENT} --query "SET optimize_on_insert; SELECT getSetting('optimize_on_insert');"

# Test against String Setting
if ${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query "SET default_database_engine;" 2>/dev/null; then
    echo "UNEXPECTED: String setting should have failed"
else
    echo "EXPECTED: String setting failed with exit code $?"
fi

 
# Test against Uint64 Setting
if ${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query "SET max_threads;" 2>/dev/null; then
    echo "UNEXPECTED: UInt64 setting should have failed"
else
    echo "EXPECTED: UInt64 setting failed with exit code $?"
fi


# Test against Seconds Setting
if ${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query "SET max_execution_time;" 2>/dev/null; then
    echo "UNEXPECTED: Seconds setting should have failed"
else
    echo "EXPECTED: Seconds setting failed with exit code $?"
fi
# Test works normal syntax works
${CLICKHOUSE_CLIENT} --query "SET max_threads = 4; SELECT getSetting('max_threads');"
