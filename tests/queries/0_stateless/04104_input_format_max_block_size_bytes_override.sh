#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_ifmbs_override"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_ifmbs_override (id UInt64) ENGINE = MergeTree() ORDER BY id
        SETTINGS parts_to_throw_insert = 10000
"

url="${CLICKHOUSE_URL}&async_insert=0&min_insert_block_size_rows=0&min_insert_block_size_bytes=0&max_insert_block_size_bytes=0&max_insert_block_size_rows=100000000&input_format_max_block_size_bytes=8"

echo -ne '1\n2\n3\n4\n5\n6\n7\n8\n' | \
    ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+t_ifmbs_override+FORMAT+CSV" --data-binary @-

for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    count=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query LIKE 'INSERT INTO t_ifmbs_override FORMAT CSV%'
          AND type = 'QueryFinish'")
    [ "${count:-0}" -ge 1 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM system.part_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 't_ifmbs_override'
      AND event_type = 'NewPart'"


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_ifmbs_override"
