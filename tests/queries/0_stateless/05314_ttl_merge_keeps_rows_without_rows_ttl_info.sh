#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge applies the rows TTL to the rows of a part that has no rows TTL info instead of dropping them: a part
# attached from a table with other TTLs, or a part written before REMOVE TTL + MODIFY TTL with
# materialize_ttl_after_modify = 0. No row kept below has reached its table's rows TTL.

function wait_for_merge()
{
    local table=$1 count
    for _ in $(seq 1 300); do
        count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active") || return
        [ "$count" -le 1 ] && break
        sleep 0.2
    done
    for _ in $(seq 1 50); do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log" || return
        count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'") || return
        [ "$count" -ge 1 ] && break
        sleep 0.2
    done
}

# Rows and parts left, and the reasons of the merges that ran.
function report()
{
    local arm=$1 table=$2
    wait_for_merge "$table"
    $CLICKHOUSE_CLIENT -q "
        SELECT '$arm', count(), uniqExact(_part) FROM $table;
        SELECT '$arm', arraySort(groupUniqArray(merge_reason)) FROM system.part_log
        WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts';"
}

# Rows and parts left, and the algorithm of the first merge that ran.
function report_algorithm()
{
    local arm=$1 table=$2
    wait_for_merge "$table"
    $CLICKHOUSE_CLIENT -q "
        SELECT '$arm', count(), uniqExact(_part) FROM $table;
        SELECT '$arm', argMin(merge_algorithm, event_time_microseconds) FROM system.part_log
        WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts';"
}

# TTL merges of other tests must not turn these merges into regular ones.
SETTINGS="merge_with_ttl_timeout = 0, max_number_of_merges_with_ttl_in_pool = 100, min_bytes_for_wide_part = 1"

# A: the parts of a table with a column TTL, attached to a table with only a rows TTL.
# M: the same parts, attached next to a native part whose rows have all expired.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_col_ttl (d DateTime, x UInt64, s String TTL d + INTERVAL 1 HOUR)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() SETTINGS $SETTINGS;
    CREATE TABLE t_a (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 50 YEAR SETTINGS $SETTINGS;
    CREATE TABLE t_m (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 1 YEAR SETTINGS $SETTINGS;

    SYSTEM STOP MERGES t_col_ttl;
    SYSTEM STOP MERGES t_a;
    SYSTEM STOP MERGES t_m;

    INSERT INTO t_col_ttl SELECT now() - INTERVAL 10 DAY, number, 'keep' FROM numbers(100);
    INSERT INTO t_col_ttl SELECT now() - INTERVAL 10 DAY, number + 100, 'keep' FROM numbers(100);
    INSERT INTO t_m SELECT now() - INTERVAL 2 YEAR, number + 1000, 'expired' FROM numbers(100);

    ALTER TABLE t_a ATTACH PARTITION tuple() FROM t_col_ttl;
    ALTER TABLE t_m ATTACH PARTITION tuple() FROM t_col_ttl;
"

# B, W, G: a column, DELETE WHERE or GROUP BY TTL replaced by a rows TTL without materializing it.
# B and its ReplicatedMergeTree twin R also hold 10 rows that the new rows TTL does remove. R is a single part,
# because a replicated table assigns merges while its merges are stopped.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_b (d DateTime, x UInt64, s String TTL d + INTERVAL 1 HOUR)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() SETTINGS $SETTINGS;
    CREATE TABLE t_r (d DateTime, x UInt64, s String TTL d + INTERVAL 1 HOUR)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_r', 'r1') ORDER BY x PARTITION BY tuple()
    SETTINGS $SETTINGS, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 1000;
    CREATE TABLE t_w (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 1 HOUR DELETE WHERE x % 2 = 0 SETTINGS $SETTINGS;
    CREATE TABLE t_g (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 1 HOUR GROUP BY x SET d = max(d), s = any(s)
    SETTINGS $SETTINGS;

    SYSTEM STOP MERGES t_b;
    SYSTEM STOP MERGES t_r;
    SYSTEM STOP MERGES t_w;
    SYSTEM STOP MERGES t_g;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_b SELECT if(number < 10, toDateTime('1971-01-01 00:00:00'), now() - INTERVAL 10 DAY), number, 'keep' FROM numbers(110);
    INSERT INTO t_b SELECT now() - INTERVAL 10 DAY, number + 110, 'keep' FROM numbers(100);
    INSERT INTO t_r SELECT if(number < 10, toDateTime('1971-01-01 00:00:00'), now() - INTERVAL 10 DAY), number, 'keep' FROM numbers(210);
    ALTER TABLE t_b MODIFY COLUMN s REMOVE TTL;
    ALTER TABLE t_r MODIFY COLUMN s REMOVE TTL;"
for t in t_w t_g; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $t SELECT now() - INTERVAL 10 DAY, number, 'keep' FROM numbers(100);
        INSERT INTO $t SELECT now() - INTERVAL 10 DAY, number + 100, 'keep' FROM numbers(100);"
done
for t in t_b t_r t_w t_g; do
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $t MODIFY TTL d + INTERVAL 50 YEAR SETTINGS materialize_ttl_after_modify = 0"
done

for t in t_a t_m t_b t_r t_w t_g; do
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES $t"
done

report A t_a
report M t_m
report B t_b
report R t_r
report W t_w
report G t_g

# U-h, U-v: OPTIMIZE FINAL merges a native part whose rows have all expired with a part attached from a table
# without TTL, through the horizontal and the vertical merge algorithm. TTL merges are kept from dropping the
# native part on its own.
U_SETTINGS="min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_no_ttl (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() SETTINGS $U_SETTINGS;
    CREATE TABLE t_uh (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 1 YEAR
    SETTINGS $U_SETTINGS, max_number_of_merges_with_ttl_in_pool = 0, enable_vertical_merge_algorithm = 0;
    CREATE TABLE t_uv (d DateTime, x UInt64, s String)
    ENGINE = MergeTree ORDER BY x PARTITION BY tuple() TTL d + INTERVAL 1 YEAR
    SETTINGS $U_SETTINGS, max_number_of_merges_with_ttl_in_pool = 0, enable_vertical_merge_algorithm = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_optimize_ttl_delete = 1;

    SYSTEM STOP MERGES t_no_ttl;
    SYSTEM STOP MERGES t_uh;
    SYSTEM STOP MERGES t_uv;

    INSERT INTO t_no_ttl SELECT now() - INTERVAL 10 DAY, number, 'keep' FROM numbers(100);
    INSERT INTO t_uh SELECT now() - INTERVAL 2 YEAR, number + 1000, 'expired' FROM numbers(100);
    INSERT INTO t_uv SELECT now() - INTERVAL 2 YEAR, number + 1000, 'expired' FROM numbers(100);

    ALTER TABLE t_uh ATTACH PARTITION tuple() FROM t_no_ttl;
    ALTER TABLE t_uv ATTACH PARTITION tuple() FROM t_no_ttl;
"
for t in t_uh t_uv; do
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES $t; OPTIMIZE TABLE $t FINAL;"
done

report_algorithm U-h t_uh
report_algorithm U-v t_uv

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_col_ttl; DROP TABLE t_a; DROP TABLE t_m; DROP TABLE t_b; DROP TABLE t_r; DROP TABLE t_w; DROP TABLE t_g;
    DROP TABLE t_no_ttl; DROP TABLE t_uh; DROP TABLE t_uv;"
