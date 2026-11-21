#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# The diagnostic must infer using the format selected by input settings, not the stale FORMAT clause.

phrase='does not match the structure expected by the query'

echo '-- client --input-format override for inline data'
$CLICKHOUSE_CLIENT -q 'CREATE TABLE test_resolved_format (a UInt8, b UInt8) ENGINE = Memory'
printf 'INSERT INTO test_resolved_format FORMAT CSV\n1\tbad\n' \
    | $CLICKHOUSE_CLIENT --input-format TSV 2>&1 | grep -F -q "$phrase" && echo 'explanation present' || echo 'explanation missing'

echo '-- async INSERT SETTINGS input_format override'
$CLICKHOUSE_CLIENT -q 'CREATE TABLE test_resolved_format_async (a UInt8, b UInt8) ENGINE = Memory'
printf "%s\n" $'INSERT INTO test_resolved_format_async SETTINGS input_format = \'TSV\' FORMAT CSV\n1\tbad' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" --data-binary @- 2>&1 \
    | grep -F -q "$phrase" && echo 'explanation present' || echo 'explanation missing'

$CLICKHOUSE_CLIENT -q 'DROP TABLE test_resolved_format'
$CLICKHOUSE_CLIENT -q 'DROP TABLE test_resolved_format_async'
