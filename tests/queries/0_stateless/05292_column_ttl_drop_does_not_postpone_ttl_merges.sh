#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A merge that drops a column fully expired by TTL from a single part does not postpone the other TTL
# merges of the partition by `merge_with_ttl_timeout`, like a merge that drops a whole part. So every part
# of the partition gets its expired column dropped right away, not one part per `merge_with_ttl_timeout`.
# `min_parts_to_merge_at_once` keeps regular merges, which would drop the column as well, from running, and
# the column `later` keeps the parts from expiring as a whole, which would make `TTLDrop` merge them.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_ttl_column_drop
    (
        d Date,
        key UInt64,
        value String TTL d + INTERVAL 1 DAY,
        later String TTL d + INTERVAL 100 YEAR
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 3600,
        min_parts_to_merge_at_once = 100;

    SYSTEM STOP MERGES t_ttl_column_drop;
    INSERT INTO t_ttl_column_drop VALUES ('2020-01-01', 1, 'a', 'x');
    INSERT INTO t_ttl_column_drop VALUES ('2020-01-01', 2, 'b', 'x');
    INSERT INTO t_ttl_column_drop VALUES ('2020-01-01', 3, 'c', 'x');
    SYSTEM START MERGES t_ttl_column_drop;
"

for _ in {1..600}; do
    remaining=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_ttl_column_drop' AND active AND column = 'value'")
    [[ $remaining == 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_column_drop' AND active;
    SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ttl_column_drop' AND active AND column = 'value';
    SELECT key, value, later FROM t_ttl_column_drop ORDER BY key;
    DROP TABLE t_ttl_column_drop;
"
