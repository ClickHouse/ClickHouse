#!/usr/bin/env bash

# A row whose DELETE or column TTL is 0 never expires, even when every other row of its part has expired.
# A column TTL of 0 does not keep a part whose rows have all expired from being dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Even ids never expire (TTL 0), odd ids expired five days ago.
FIXTURE="SELECT number, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY) FROM numbers(10)"

# max_number_of_merges_with_ttl_in_pool = 0 keeps background TTL merges away, so OPTIMIZE FINAL is the merge under test.

echo "-- horizontal merge"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_horizontal (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0, enable_vertical_merge_algorithm = 0;
    INSERT INTO t_horizontal $FIXTURE;
    OPTIMIZE TABLE t_horizontal FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_horizontal;
    DROP TABLE t_horizontal;
"

echo "-- vertical merge"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_vertical (id UInt64, delete_at DateTime DEFAULT 0, a String, b String) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0, min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_ttl_delete = 1;
    INSERT INTO t_vertical SELECT number, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'a', 'b' FROM numbers(10);
    INSERT INTO t_vertical SELECT number + 10, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'a', 'b' FROM numbers(10);
    OPTIMIZE TABLE t_vertical FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_vertical;
    SYSTEM FLUSH LOGS part_log;
    SELECT groupUniqArray(merge_algorithm) FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_vertical' AND event_type = 'MergeParts';
    DROP TABLE t_vertical;
"

echo "-- TTL expression that is 0 for some rows"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_expression (id UInt64, ts DateTime, keep UInt8) ENGINE = MergeTree ORDER BY id
    TTL if(keep = 1, toDateTime(0), ts + INTERVAL 1 DAY) DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_expression SELECT number, now() - INTERVAL 5 DAY, number % 2 FROM numbers(10);
    OPTIMIZE TABLE t_expression FINAL;
    SELECT count(), countIf(keep = 1) FROM t_expression;
    DROP TABLE t_expression;
"

echo "-- column TTL"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_column (id UInt64, d DateTime DEFAULT 0, v String DEFAULT 'dflt' TTL d) ENGINE = MergeTree ORDER BY id
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_column SELECT number, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'x' FROM numbers(10);
    OPTIMIZE TABLE t_column FINAL;
    SELECT countIf(d = 0 AND v = 'x'), countIf(d != 0 AND v = 'dflt') FROM t_column;
    DROP TABLE t_column;
"

echo "-- MODIFY TTL with ttl_only_drop_parts"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_modify_ttl (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 0;
    INSERT INTO t_modify_ttl $FIXTURE;
    ALTER TABLE t_modify_ttl MODIFY TTL delete_at DELETE SETTINGS mutations_sync = 2;
    SELECT count(), countIf(delete_at = 0) FROM t_modify_ttl;
    SELECT delete_ttl_info_min > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_modify_ttl' AND active;
    DROP TABLE t_modify_ttl;
"

echo "-- part with only TTL 0 rows, reloaded, then merged with an expired part"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_reload (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_reload SELECT number, toDateTime(0) FROM numbers(5);
    DETACH TABLE t_reload;
    ATTACH TABLE t_reload;
    INSERT INTO t_reload SELECT number + 100, now() - INTERVAL 5 DAY FROM numbers(5);
    OPTIMIZE TABLE t_reload FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_reload;
    DROP TABLE t_reload;
"

echo "-- the same with a second TTL rule, whose TTL information must survive the reload too"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_reload_two_rules (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id
    TTL delete_at DELETE, delete_at + INTERVAL 1 DAY RECOMPRESS CODEC(LZ4HC)
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_reload_two_rules SELECT number, toDateTime(0) FROM numbers(5);
    DETACH TABLE t_reload_two_rules;
    ATTACH TABLE t_reload_two_rules;
    SELECT length(recompression_ttl_info.expression) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_reload_two_rules' AND active;
    INSERT INTO t_reload_two_rules SELECT number + 100, now() - INTERVAL 5 DAY FROM numbers(5);
    OPTIMIZE TABLE t_reload_two_rules FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_reload_two_rules;
    DROP TABLE t_reload_two_rules;
"

echo "-- merged twice"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_twice (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_twice $FIXTURE;
    OPTIMIZE TABLE t_twice FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_twice;
    INSERT INTO t_twice SELECT number + 100, now() - INTERVAL 5 DAY FROM numbers(5);
    OPTIMIZE TABLE t_twice FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_twice;
    DROP TABLE t_twice;
"

echo "-- column TTL, merged twice"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_column_twice (id UInt64, d DateTime DEFAULT 0, v String DEFAULT 'dflt' TTL d) ENGINE = MergeTree ORDER BY id
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_column_twice SELECT number, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'x' FROM numbers(10);
    OPTIMIZE TABLE t_column_twice FINAL;
    SELECT countIf(d = 0 AND v = 'x'), countIf(d != 0 AND v = 'dflt') FROM t_column_twice;
    INSERT INTO t_column_twice SELECT number + 100, now() - INTERVAL 5 DAY, 'x' FROM numbers(5);
    OPTIMIZE TABLE t_column_twice FINAL;
    SELECT countIf(d = 0 AND v = 'x'), countIf(d != 0 AND v = 'dflt') FROM t_column_twice;
    DROP TABLE t_column_twice;
"

echo "-- column TTL added by MODIFY COLUMN, recalculated only"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_modify_column (id UInt64, d DateTime DEFAULT 0, v String DEFAULT 'dflt') ENGINE = MergeTree ORDER BY id
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0, min_bytes_for_wide_part = 0,
        materialize_ttl_recalculate_only = 1;
    INSERT INTO t_modify_column SELECT number, if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'x' FROM numbers(10);
    ALTER TABLE t_modify_column MODIFY COLUMN v String DEFAULT 'dflt' TTL d SETTINGS mutations_sync = 2;
    OPTIMIZE TABLE t_modify_column FINAL;
    SELECT countIf(d = 0 AND v = 'x'), countIf(d != 0 AND v = 'dflt') FROM t_modify_column;
    DROP TABLE t_modify_column;
"

echo "-- ttl_only_drop_parts drops the expired rest of a part once ALTER DELETE removes its rows whose TTL is 0"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_delete_zero (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_delete_zero $FIXTURE;
    ALTER TABLE t_delete_zero DELETE WHERE delete_at = 0 SETTINGS mutations_sync = 2;
    SELECT count() FROM t_delete_zero;
    DROP TABLE t_delete_zero;
"

echo "-- a merge that replaces the only row whose TTL is 0 recalculates the bound of the rest"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_replace_zero (id UInt64, v UInt64, delete_at DateTime) ENGINE = ReplacingMergeTree(v) ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_replace_zero VALUES (0, 1, 0), (1, 1, '2100-01-01 00:00:00');
    INSERT INTO t_replace_zero VALUES (0, 2, '2100-01-01 00:00:00');
    OPTIMIZE TABLE t_replace_zero FINAL;
    SELECT rows, delete_ttl_info_max = toDateTime('2100-01-01 00:00:00') FROM system.parts
    WHERE database = currentDatabase() AND table = 't_replace_zero' AND active;
    DROP TABLE t_replace_zero;
"

echo "-- a DELETE WHERE rule that removes the only row whose TTL is 0 recalculates the bound of the rest"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_where_zero (id UInt64, delete_at DateTime, erase_at DateTime) ENGINE = MergeTree ORDER BY id
    TTL delete_at DELETE, erase_at DELETE WHERE id = 0
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_where_zero VALUES (0, 0, 1), (1, '2100-01-01 00:00:00', 0);
    OPTIMIZE TABLE t_where_zero FINAL;
    SELECT rows, delete_ttl_info_max = toDateTime('2100-01-01 00:00:00') FROM system.parts
    WHERE database = currentDatabase() AND table = 't_where_zero' AND active;
    DROP TABLE t_where_zero;
"

echo "-- the same for a part whose TTL was not calculated before the merge"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_where_zero_not_calculated (id UInt64, delete_at DateTime, erase_at DateTime) ENGINE = MergeTree ORDER BY id
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_where_zero_not_calculated VALUES (0, 0, 1), (1, '2100-01-01 00:00:00', 0);
    ALTER TABLE t_where_zero_not_calculated MODIFY TTL delete_at DELETE, erase_at DELETE WHERE id = 0
    SETTINGS materialize_ttl_after_modify = 0;
    OPTIMIZE TABLE t_where_zero_not_calculated FINAL;
    SELECT rows, delete_ttl_info_max = toDateTime('2100-01-01 00:00:00') FROM system.parts
    WHERE database = currentDatabase() AND table = 't_where_zero_not_calculated' AND active;
    DROP TABLE t_where_zero_not_calculated;
"

echo "-- a GROUP BY rule that folds the only row whose TTL is 0 recalculates the bound of the rest"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_group_by_zero (k UInt64, id UInt64, delete_at DateTime, group_at DateTime) ENGINE = MergeTree ORDER BY (k, id)
    TTL delete_at DELETE, group_at GROUP BY k SET delete_at = max(delete_at)
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_group_by_zero VALUES (1, 0, 0, 1), (1, 1, '2100-01-01 00:00:00', 1);
    OPTIMIZE TABLE t_group_by_zero FINAL;
    SELECT rows, delete_ttl_info_max = toDateTime('2100-01-01 00:00:00') FROM system.parts
    WHERE database = currentDatabase() AND table = 't_group_by_zero' AND active;
    DROP TABLE t_group_by_zero;
"

echo "-- a GROUP BY rule that sets the TTL of the folded row to 0 makes the bound never expire"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_group_by_set_zero (k UInt64, id UInt64, delete_at DateTime, zero_at DateTime, group_at DateTime)
    ENGINE = MergeTree ORDER BY (k, id) TTL delete_at DELETE, group_at GROUP BY k SET delete_at = min(zero_at)
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_group_by_set_zero SELECT 1, number, '2100-01-01 00:00:00', 0, 1 FROM numbers(2);
    OPTIMIZE TABLE t_group_by_set_zero FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_group_by_set_zero;
    SELECT toUInt32(delete_ttl_info_max) = 4294967295 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_group_by_set_zero' AND active;
    DROP TABLE t_group_by_set_zero;
"

echo "-- a row whose TTL a column TTL resets to 0 never expires"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_column_reset (id UInt64, delete_at DateTime DEFAULT 0 TTL reset_at, reset_at DateTime) ENGINE = MergeTree ORDER BY id
    TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_column_reset VALUES (1, '2100-01-01 00:00:00', 1);
    OPTIMIZE TABLE t_column_reset FINAL;
    SELECT count(), countIf(delete_at = 0) FROM t_column_reset;
    -- NEVER, which system.parts shows as the largest DateTime.
    SELECT toUInt32(delete_ttl_info_max) = 4294967295 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_column_reset' AND active;
    DROP TABLE t_column_reset;
"

echo "-- the same when the rows TTL reads a subcolumn of the reset column"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_column_reset_subcolumn (id UInt64, t Tuple(d DateTime) TTL reset_at, reset_at DateTime) ENGINE = MergeTree ORDER BY id
    TTL t.d DELETE
    SETTINGS merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_column_reset_subcolumn SELECT 1, tuple(toDateTime('2100-01-01 00:00:00')), toDateTime(1);
    OPTIMIZE TABLE t_column_reset_subcolumn FINAL;
    SELECT toUInt32(delete_ttl_info_max) = 4294967295 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_column_reset_subcolumn' AND active;
    DROP TABLE t_column_reset_subcolumn;
"

# max_number_of_merges_with_ttl_in_pool is compared with the number of TTL merges of the whole server, so the tables
# below that wait for a background TTL merge raise it to 100. Where the part must be prepared first, it stays 0 until then.

echo "-- background TTL merge"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_background (id UInt64, delete_at DateTime DEFAULT 0) ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 0, max_number_of_merges_with_ttl_in_pool = 100;
    INSERT INTO t_background $FIXTURE;
"
# Wait until the inserted part has been merged away.
for _ in $(seq 1 300); do
    unmerged=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_background' AND active AND level = 0")
    [ "$unmerged" = "0" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "
    SELECT count(), countIf(delete_at = 0) FROM t_background;
    DROP TABLE t_background;
"

echo "-- ttl_only_drop_parts drops an expired part whose column TTL is 0 for every row"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_drop_column_zero (id UInt64, ts DateTime, v_expire DateTime DEFAULT 0, v String TTL v_expire)
    ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 100;
    INSERT INTO t_drop_column_zero (id, ts, v) SELECT number, now() - INTERVAL 5 DAY, 'x' FROM numbers(10);
"
for _ in $(seq 1 300); do
    unmerged=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_column_zero' AND active AND level = 0")
    [ "$unmerged" = "0" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_drop_column_zero;
    SYSTEM FLUSH LOGS part_log;
    SELECT merge_reason FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_drop_column_zero' AND event_type = 'MergeParts';
    DROP TABLE t_drop_column_zero;
"

echo "-- rows whose TTL is 0 survive the drop of an expired column TTL that was removed"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_removed_column_ttl (id UInt64, delete_at DateTime DEFAULT 0, ts DateTime, v String TTL ts + INTERVAL 1 DAY)
    ENGINE = MergeTree ORDER BY id TTL delete_at DELETE
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 0, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_removed_column_ttl SELECT number, toDateTime(0), now() - INTERVAL 5 DAY, 'x' FROM numbers(10);
    ALTER TABLE t_removed_column_ttl MODIFY COLUMN v REMOVE TTL;
    ALTER TABLE t_removed_column_ttl MODIFY SETTING max_number_of_merges_with_ttl_in_pool = 100;
"
for _ in $(seq 1 300); do
    unmerged=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_removed_column_ttl' AND active AND level = 0")
    [ "$unmerged" = "0" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "
    SELECT count(), countIf(delete_at = 0) FROM t_removed_column_ttl;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() > 0 FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_removed_column_ttl' AND event_type = 'MergeParts';
    DROP TABLE t_removed_column_ttl;
"

echo "-- ttl_only_drop_parts drops an expired part after a column TTL that is 0 for some rows is removed, also after a restart"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_removed_mixed_column_ttl (id UInt64, ts DateTime, v_expire DateTime, v String TTL v_expire)
    ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY
    SETTINGS merge_with_ttl_timeout = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0;
    INSERT INTO t_removed_mixed_column_ttl SELECT number, now() - INTERVAL 5 DAY,
        if(number % 2 = 0, toDateTime(0), now() - INTERVAL 5 DAY), 'x' FROM numbers(10);
    ALTER TABLE t_removed_mixed_column_ttl MODIFY COLUMN v REMOVE TTL;
    DETACH TABLE t_removed_mixed_column_ttl;
    ATTACH TABLE t_removed_mixed_column_ttl;
    ALTER TABLE t_removed_mixed_column_ttl MODIFY SETTING max_number_of_merges_with_ttl_in_pool = 100;
"
for _ in $(seq 1 300); do
    unmerged=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_removed_mixed_column_ttl' AND active AND level = 0")
    [ "$unmerged" = "0" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_removed_mixed_column_ttl;
    SYSTEM FLUSH LOGS part_log;
    SELECT merge_reason FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_removed_mixed_column_ttl' AND event_type = 'MergeParts';
    DROP TABLE t_removed_mixed_column_ttl;
"
