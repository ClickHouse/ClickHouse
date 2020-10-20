#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

EXCEPTION_SUCCESS_TEXT=ok

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_drop_indices;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_drop_indices
(
        a       UInt32,
        b       UInt32
)
ENGINE = MergeTree ORDER BY (a);"

# Must throw an exception
EXCEPTION_TEXT="Cannot find index \`test_index\` to drop.."
$CLICKHOUSE_CLIENT --query="ALTER TABLE test_drop_indices DROP INDEX test_index;" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

# Must not throw an exception
$CLICKHOUSE_CLIENT --query="ALTER TABLE test_drop_indices DROP INDEX IF EXISTS test_index"

$CLICKHOUSE_CLIENT --query="DROP TABLE test_drop_indices;"

