#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_ifmbs_override"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_ifmbs_override (id UInt64) ENGINE = MergeTree() ORDER BY id
        SETTINGS parts_to_throw_insert = 10000
"

# Use a unique query_id so the system.part_log assertion below counts only the parts
# created by this exact INSERT. Matching by table and a time window is racy: a re-run of
# the test reuses the same database and table, so stale NewPart rows from a previous run
# could be counted.
query_id="04104_$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")"

url="${CLICKHOUSE_URL}&async_insert=0&min_insert_block_size_rows=0&min_insert_block_size_bytes=0&max_insert_block_size_bytes=0&max_insert_block_size_rows=100000000&input_format_max_block_size_bytes=8&query_id=${query_id}"

# input_format_max_block_size_bytes=8 limits each parser block to a single UInt64 row (8 bytes),
# and squashing is disabled, so the 8 input rows are expected to become 8 separate parts.
echo -ne '1\n2\n3\n4\n5\n6\n7\n8\n' | \
    ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+t_ifmbs_override+FORMAT+CSV" --data-binary @-

# The query_log/part_log entries are written after the HTTP response is sent, so there is a
# race between the curl request returning and the rows becoming queryable
# (https://github.com/ClickHouse/ClickHouse/issues/84364). Flush and poll until the INSERT
# appears in query_log as QueryFinish; by then all of its NewPart events are flushed too.
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, part_log"
    finished=$($CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND current_database = currentDatabase()
          AND query_id = '${query_id}'
          AND type = 'QueryFinish'")
    [ "$finished" -ge 1 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM system.part_log
    WHERE event_date >= yesterday()
      AND query_id = '${query_id}'
      AND event_type = 'NewPart'"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_ifmbs_override"
