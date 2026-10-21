#!/usr/bin/env bash
# Tags: long
# long: many statements over thousands of rows; over the time limit of the flaky check under the thread fuzzer.

# Two parts of 6 000 rows with a dozen SHARED paths of mixed types: several tasks per part, readers reused across tasks,
# many small ranges per task, FINAL over intersecting parts, merges and mutations, and the read settings that pick
# another read buffer or prefix cache; compared with a Memory table of the type without SHARED.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PATHS="a UInt64, b Int32, c Float32, d String, e Array(UInt8), f Nullable(UInt32), g LowCardinality(String), h Dynamic, i.x UInt64, i.y String, j Bool"
SHARED="JSON(SHARED ${PATHS//, /, SHARED }, max_dynamic_paths = 4)"
PLAIN="JSON($PATHS, max_dynamic_paths = 4)"
ROW="concat('{\"a\":', toString(number), ',\"b\":', toString(toInt32(number % 1000) - 500), ',\"c\":', toString(number / 7), ',\"d\":\"', repeat('s', number % 4), '\",\"e\":', toString(range(number % 3)), if(number % 5 = 0, ',\"f\":null', concat(',\"f\":', toString(number))), ',\"g\":\"g', toString(number % 10), '\"', if(number % 2 = 0, concat(',\"h\":', toString(number)), concat(',\"h\":\"h', toString(number % 7), '\"')), ',\"i\":{\"x\":', toString(number * 2), ',\"y\":\"', repeat('y', number % 3), '\"},\"j\":', if(number % 3 = 0, 'true', 'false'), if(number % 2 = 0, concat(',\"m\":', toString(number)), ''), if(number % 11 = 0, concat(',\"n\":\"n', toString(number), '\"'), ''), '}')"
# Granules of 128 rows; a vertical merge on OPTIMIZE.
LAYOUT="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 128, index_granularity_bytes = 10485760, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1"
# Tasks of 5 granules, so that every thread's reader serves several tasks.
BASE="optimize_move_to_prewhere = 0, use_query_condition_cache = 0, merge_tree_min_rows_for_concurrent_read = 640, merge_tree_min_bytes_for_concurrent_read = 1"
TWIN=1

# The read lists of a section are collected and sent in one client call: a client start is expensive under sanitizers.
Q=""
run_batch() { $CLICKHOUSE_CLIENT -n -q "$Q"; Q=""; }

reads() # table, label, settings, [condition]: the read list against the twin, EXCEPT counts and aggregates
{
    local table=$1 label=$2 settings="$BASE, $3" cond=${4:-1}
    local agg_paths="(sum(json.b), sum(length(json.d)), uniqExact(json.g), countIf(json.j), sum(round(json.c * 7)))"
    local agg_sub="(sum(json.e.size0), sum(arraySum(json.e)), countIf(json.f IS NULL), sum(json.f), countIf(json.h.:Int64 IS NOT NULL), uniqExact(json.h.:String), sum(json.m.:Int64), countIf(json.n IS NOT NULL), sum(json.i.x))"
    Q+="
    SELECT '$label json', (SELECT count() FROM (SELECT x, toString(json) FROM $table WHERE $cond SETTINGS $settings EXCEPT SELECT x, toString(json) FROM mem WHERE ($cond) AND ($TWIN))), (SELECT count() FROM (SELECT x, toString(json) FROM mem WHERE ($cond) AND ($TWIN) EXCEPT SELECT x, toString(json) FROM $table WHERE $cond SETTINGS $settings));
    SELECT '$label json.a', (SELECT (count(), sum(json.a), uniqExact(json.a)) FROM $table WHERE $cond SETTINGS $settings) = (SELECT (count(), sum(json.a), uniqExact(json.a)) FROM mem WHERE ($cond) AND ($TWIN));
    SELECT '$label three paths', (SELECT $agg_paths FROM $table WHERE $cond SETTINGS $settings) = (SELECT $agg_paths FROM mem WHERE ($cond) AND ($TWIN));
    SELECT '$label sub-object', (SELECT count() FROM (SELECT x, toString(json.^i) FROM $table WHERE $cond SETTINGS $settings EXCEPT SELECT x, toString(json.^i) FROM mem WHERE ($cond) AND ($TWIN)));
    SELECT '$label paths with subcolumns', (SELECT $agg_sub FROM $table WHERE $cond SETTINGS $settings) = (SELECT $agg_sub FROM mem WHERE ($cond) AND ($TWIN));
    SELECT '$label (json, json.a)', (SELECT count() FROM (SELECT x, toString(json), json.a FROM $table WHERE $cond SETTINGS $settings EXCEPT SELECT x, toString(json), json.a FROM mem WHERE ($cond) AND ($TWIN)));
    "
}

echo "-- two parts, tasks of 5 granules, 8 and 16 threads"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS mem; DROP TABLE IF EXISTS t;
CREATE TABLE mem (x UInt64, y UInt64, json $PLAIN) ENGINE = Memory;
INSERT INTO mem SELECT number, number, $ROW FROM numbers(12000);
CREATE TABLE t (x UInt64, y UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $LAYOUT;
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number, number, $ROW FROM numbers(6000);
INSERT INTO t SELECT number, number, $ROW FROM numbers(6000, 6000);
SELECT 'parts', count(), sum(rows), sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
"
reads t "t, 8 threads" "max_threads = 8"
reads t "t, 16 threads" "max_threads = 16"
run_batch

echo "-- many small ranges per task, scattered rows and 16 disjoint ranges"
scattered=$(seq -s ', ' 7 239 11999)
ranges=""
for i in $(seq 0 15); do ranges+=" OR x BETWEEN $((i * 720 + 37)) AND $((i * 720 + 180))"; done
reads t "t, scattered rows" "max_threads = 8" "x IN ($scattered)"
reads t "t, 16 ranges" "max_threads = 8" "${ranges# OR }"
run_batch

echo "-- FINAL over four intersecting parts under the range split injection"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS rt;
CREATE TABLE rt (x UInt64, y UInt64, json $SHARED) ENGINE = ReplacingMergeTree ORDER BY x SETTINGS $LAYOUT;
SYSTEM STOP MERGES rt;
INSERT INTO rt SELECT * FROM t WHERE x % 4 = 0;
INSERT INTO rt SELECT * FROM t WHERE x % 4 = 1;
INSERT INTO rt SELECT * FROM t WHERE x % 4 = 2;
INSERT INTO rt SELECT * FROM t WHERE x % 4 = 3;
SELECT 'rt parts', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'rt' AND active;
"
for vertical in 0 1; do
    reads "rt FINAL" "rt FINAL, vertical final $vertical" "max_threads = 8, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.5, enable_vertical_final = $vertical"
done
run_batch

echo "-- the read settings that pick another buffer, cache or prefix handling"
reads t "t, uncompressed cache" "max_threads = 8, use_uncompressed_cache = 1"
reads t "t, pread_threadpool" "max_threads = 8, local_filesystem_read_method = 'pread_threadpool'"
reads t "t, mmap" "max_threads = 8, min_bytes_to_use_mmap_io = 1"
for cache in 0 1; do
    reads t "t, prefixes cache $cache, prefixes pool $cache" "max_threads = 16, merge_tree_use_deserialization_prefixes_cache = $cache, merge_tree_use_prefixes_deserialization_thread_pool = $cache"
done
run_batch

echo "-- the same rows under constant granularity"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_const;
CREATE TABLE t_const (x UInt64, y UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $LAYOUT, index_granularity_bytes = 0;
INSERT INTO t_const SELECT * FROM t;
"
reads t_const "t_const" "max_threads = 16"
run_batch

echo "-- after a vertical merge, a lightweight delete and an update of another column"
$CLICKHOUSE_CLIENT -n -q "
SYSTEM START MERGES t;
OPTIMIZE TABLE t FINAL;
SELECT 'merged', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
"
reads t "t merged" "max_threads = 16"
run_batch
$CLICKHOUSE_CLIENT -n -q "DELETE FROM t WHERE x % 97 = 0;"
TWIN="x % 97 != 0"
reads t "t after the delete" "max_threads = 16"
run_batch
$CLICKHOUSE_CLIENT -n -q "ALTER TABLE t UPDATE y = y + 1 WHERE 1 SETTINGS mutations_sync = 2;"
reads t "t after the update" "max_threads = 8"
run_batch
$CLICKHOUSE_CLIENT -q "SELECT 'y updated', sum(y) = sum(x) + count() FROM t"

echo "-- memory: the whole-column read at 16 threads stays under 4 GiB"
$CLICKHOUSE_CLIENT -n -q "
SELECT json FROM t FORMAT Null SETTINGS $BASE, max_threads = 16, log_comment = '05305_whole_column';
SYSTEM FLUSH LOGS query_log;
SELECT memory_usage < 4 * 1024 * 1048576 FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05305_whole_column' ORDER BY event_time_microseconds DESC LIMIT 1;
"

echo "-- the column rewritten by a mutation, read at 16 threads"
$CLICKHOUSE_CLIENT -n -q "ALTER TABLE t UPDATE json = json WHERE 1 SETTINGS mutations_sync = 2;"
reads t "t rewritten" "max_threads = 16"
run_batch
$CLICKHOUSE_CLIENT -n -q "DROP TABLE mem; DROP TABLE t; DROP TABLE rt; DROP TABLE t_const;"
