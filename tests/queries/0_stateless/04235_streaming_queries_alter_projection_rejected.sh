#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-old-analyzer
# no-shared-merge-tree: STREAM reads are only exercised on plain MergeTree here, like the other streaming .sh tests.
# no-old-analyzer: SELECT ... STREAM throws NOT_IMPLEMENTED, so no subscription is held and the ALTER is accepted.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_alter_projection"
$STREAMING_CLIENT -q "CREATE TABLE t_streaming_alter_projection (a String, b UInt64, PROJECTION p (SELECT b ORDER BY b) WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
$STREAMING_CLIENT -q "INSERT INTO t_streaming_alter_projection VALUES ('started', 0)"

read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_alter_projection STREAM")
read_until "$fifo" "started" > /dev/null

echo "=== MODIFY PROJECTION is rejected while a streaming query holds a subscription ==="
# `-- { serverError ... }` is only available in .sql tests, so assert on the error
# code name the way the other .sh tests do. On any other outcome print the whole
# error instead of swallowing it, so a failure diff shows what actually happened.
alter_error=$($STREAMING_CLIENT -q "ALTER TABLE t_streaming_alter_projection MODIFY PROJECTION p (SELECT b ORDER BY b) WITH SETTINGS (index_granularity = 128)" 2>&1 >/dev/null)
if echo "$alter_error" | grep -qF "SUPPORT_IS_DISABLED"; then
    echo "SUPPORT_IS_DISABLED"
else
    echo "UNEXPECTED: ${alter_error:-the ALTER was accepted}"
fi

echo "=== a settings-only ALTER is still accepted ==="
$STREAMING_CLIENT -q "ALTER TABLE t_streaming_alter_projection MODIFY SETTING merge_max_block_size = 8192" 2>&1 && echo "ok"

cleanup "$fifo" "$pid" > /dev/null

echo "=== the projection settings are unchanged ==="
$STREAMING_CLIENT -q "SELECT extract(create_table_query, 'index_granularity = \d+\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_streaming_alter_projection'"

$STREAMING_CLIENT -q "DROP TABLE IF EXISTS t_streaming_alter_projection"
