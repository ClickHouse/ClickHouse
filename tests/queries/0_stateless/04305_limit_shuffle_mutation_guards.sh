#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function expect_support_is_disabled()
{
    local query="$1"
    local stderr_file="${CLICKHOUSE_TMP}/04305_limit_shuffle_mutation_guard.err"
    if $CLICKHOUSE_CLIENT --query "$query" > /dev/null 2> "${stderr_file}"
    then
        echo "Expected SUPPORT_IS_DISABLED"
        exit 1
    fi
    if ! grep -q "SUPPORT_IS_DISABLED" "${stderr_file}"
    then
        cat "${stderr_file}"
        exit 1
    fi
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE limit_shuffle_mutation_guard (number UInt64, value UInt64) ENGINE = MergeTree ORDER BY number"

expect_support_is_disabled "ALTER TABLE limit_shuffle_mutation_guard DELETE WHERE number IN (SELECT number FROM numbers(10) LIMIT 1 SHUFFLE) SETTINGS allow_experimental_shuffle_query = 1"
expect_support_is_disabled "DELETE FROM limit_shuffle_mutation_guard WHERE number IN (SELECT number FROM numbers(10) LIMIT 1 SHUFFLE) SETTINGS allow_experimental_shuffle_query = 1"
expect_support_is_disabled "UPDATE limit_shuffle_mutation_guard SET value = value WHERE number IN (SELECT number FROM numbers(10) LIMIT 1 SHUFFLE) SETTINGS allow_experimental_shuffle_query = 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE limit_shuffle_mutation_guard"

echo "OK"
