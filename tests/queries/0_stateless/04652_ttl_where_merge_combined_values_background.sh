#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge that runs while TTL merges are stopped may combine rows into one that the TTL WHERE
# matches. It must not delete it right then, but it must leave the part advertising the row as
# expirable, so that background TTL selection picks the part up afterwards. This checks the
# background path specifically - no OPTIMIZE, so the row can only disappear if
# TTLRowDeleteMergeSelector selected the part on its own.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS ttl_where_background;

    CREATE TABLE ttl_where_background
    (
        key UInt64,
        occurrences SimpleAggregateFunction(sum, Int64),
        expiry SimpleAggregateFunction(max, DateTime)
    )
    ENGINE = AggregatingMergeTree
    ORDER BY key
    TTL expiry DELETE WHERE occurrences = 0
    SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

    SYSTEM STOP TTL MERGES ttl_where_background;

    INSERT INTO ttl_where_background VALUES (1, -1, '2020-01-01 00:00:00');
    INSERT INTO ttl_where_background VALUES (1, +1, '2020-01-01 00:00:00');

    OPTIMIZE TABLE ttl_where_background FINAL;
"

# The combining merge happened with TTL merges stopped, so nothing was deleted yet.
$CLICKHOUSE_CLIENT -q "SELECT 'stopped', count() FROM ttl_where_background"

$CLICKHOUSE_CLIENT -q "SYSTEM START TTL MERGES ttl_where_background"

for _ in {1..120}
do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ttl_where_background")
    if [[ "$count" == "0" ]]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT 'started', count() FROM ttl_where_background"

$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_where_background"

# The mirror image: here the source parts do advertise an expired rows-WHERE TTL, but `expiry` is a
# max() aggregate so the merged row's TTL moves into the future. Refreshing the rows-WHERE info must
# move the part's aggregate TTL along with it, otherwise the part keeps claiming an expired 2020 TTL
# and gets selected for a TTL merge that has nothing to delete.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS ttl_where_moves_later;

    CREATE TABLE ttl_where_moves_later
    (
        key UInt64,
        occurrences SimpleAggregateFunction(sum, Int64),
        expiry SimpleAggregateFunction(max, DateTime)
    )
    ENGINE = AggregatingMergeTree
    ORDER BY key
    TTL expiry DELETE WHERE occurrences = 0
    SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

    SYSTEM STOP TTL MERGES ttl_where_moves_later;

    INSERT INTO ttl_where_moves_later VALUES (1, 0, '2020-01-01 00:00:00');
    INSERT INTO ttl_where_moves_later VALUES (1, 0, '2106-01-01 00:00:00');

    OPTIMIZE TABLE ttl_where_moves_later FINAL;

    SYSTEM START TTL MERGES ttl_where_moves_later;
"

# Give background selection a chance to act on a stale expired aggregate.
sleep 5

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'moved later', count(), countIf(merge_reason = 'TTLDeleteMerge')
    FROM system.part_log
    WHERE event_date >= yesterday() AND database = currentDatabase()
      AND table = 'ttl_where_moves_later' AND event_type = 'MergeParts'
"

$CLICKHOUSE_CLIENT -q "SELECT 'moved later rows', count() FROM ttl_where_moves_later"

$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_where_moves_later"
