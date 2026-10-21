#!/usr/bin/env bash
# Tags: long, no-fasttest, no-distributed-cache
# long: many statements and S3 round trips; over the time limit of the flaky check under the thread fuzzer.
# no-fasttest: needs the S3 disks.
# no-distributed-cache: the cache counters the test checks are zero when the distributed cache serves the reads.

# Object storage: the SHARED type read from S3 without and with the filesystem cache under both remote read methods and
# the prefetched read pool, the requests and bytes a path read and a whole-column read issue, a merge, a one-row patch
# part, and a two-part table read by many tasks per part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SHARED="JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8), SHARED d.x UInt64, SHARED d.y String)"
PLAIN="JSON(a UInt64, b String, c Array(UInt8), d.x UInt64, d.y String)"
ROW="concat('{\"a\":', toString(number * 10), ',\"b\":\"', repeat('x', number % 7), '\",\"c\":', toString(range(number % 4)), ',\"d\":{\"x\":', toString(number), ',\"y\":\"', repeat('y', number % 5), '\"}', if(number % 2 = 0, concat(',\"e\":', toString(number)), ''), '}')"
# Fixed granules of 4 rows, every substream of a granule in its own compressed block.
LAYOUT="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760, min_compress_block_size = 1, min_bytes_for_full_part_storage = 0"
NO_CACHE="enable_filesystem_cache = 0, use_page_cache_for_disks_without_file_cache = 0, use_page_cache_for_object_storage = 0, use_uncompressed_cache = 0, optimize_move_to_prewhere = 0, use_query_condition_cache = 0"

# The comparisons of a section are collected and sent in one client call: a client start is expensive under sanitizers.
Q=""
run_batch() { $CLICKHOUSE_CLIENT -n -q "$Q"; Q=""; }

compare() # table, label, query settings, [log comment]: the reads of the list compared with the Memory twin
{
    local table=$1 label=$2 settings=$3 q="" tail=""
    [ -n "${4:-}" ] && tail=" SETTINGS log_comment = '$4'"
    for select in "json" "json.a" "json.c.size0" "(json, json.a)" "(json.^d, json.d.x)"; do
        for where in "" "WHERE x >= 13"; do
            q+="SELECT '$label $select${where:+ $where}', (SELECT arraySort(groupArray((x, toString($select)))) FROM $table $where SETTINGS $settings) = (SELECT arraySort(groupArray((x, toString($select)))) FROM mem $where)$tail;"
        done
    done
    q+="SELECT '$label ORDER BY x DESC LIMIT 7', (SELECT groupArray((x, toString(json), json.a)) FROM (SELECT * FROM $table ORDER BY x DESC LIMIT 7 SETTINGS $settings)) = (SELECT groupArray((x, toString(json), json.a)) FROM (SELECT * FROM mem ORDER BY x DESC LIMIT 7))$tail;"
    Q+="$q"
}

echo "-- S3 without the filesystem cache: both remote read methods, the prefetched read pool, a packed part"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS mem; DROP TABLE IF EXISTS s3; DROP TABLE IF EXISTS s3p;
CREATE TABLE mem (x UInt64, json $PLAIN) ENGINE = Memory;
INSERT INTO mem SELECT number, $ROW FROM numbers(60);
CREATE TABLE s3 (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_no_cache', $LAYOUT;
CREATE TABLE s3p (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_no_cache', $LAYOUT, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO s3 SELECT number, $ROW FROM numbers(60) SETTINGS max_threads = 1, max_insert_threads = 1;
INSERT INTO s3p SELECT number, $ROW FROM numbers(60) SETTINGS max_threads = 1, max_insert_threads = 1;
SELECT table, part_storage_type, part_type, marks FROM system.parts WHERE database = currentDatabase() AND table IN ('s3', 's3p') AND active ORDER BY table;
"
for table in s3 s3p; do
    for method in read threadpool; do
        compare $table "$table $method" "max_threads = 1, max_block_size = 5, remote_filesystem_read_method = '$method', $NO_CACHE"
    done
done
for pool in 0 1; do
    compare s3 "s3 prefetched pool, prefixes pool $pool" "max_threads = 4, max_block_size = 5, merge_tree_min_rows_for_concurrent_read = 8, merge_tree_min_bytes_for_concurrent_read = 1, remote_filesystem_read_method = 'threadpool', allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_min_bytes_for_single_read_task = 1, merge_tree_use_prefixes_deserialization_thread_pool = $pool, $NO_CACHE"
done
run_batch

echo "-- the filesystem cache: a cold read fills it, warm reads come from it"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS s3c;
CREATE TABLE s3c (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_cache', $LAYOUT;
INSERT INTO s3c SELECT number, $ROW FROM numbers(60) SETTINGS max_threads = 1, max_insert_threads = 1, enable_filesystem_cache_on_write_operations = 0;
"
CACHE="enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, use_page_cache_for_disks_without_file_cache = 0, use_uncompressed_cache = 0, use_columns_cache = 0, optimize_move_to_prewhere = 0, use_query_condition_cache = 0"
compare s3c "s3c cold read" "max_threads = 1, max_block_size = 5, remote_filesystem_read_method = 'read', $CACHE" "05303_cold"
compare s3c "s3c warm threadpool" "max_threads = 1, max_block_size = 5, remote_filesystem_read_method = 'threadpool', $CACHE" "05303_warm_threadpool"
compare s3c "s3c warm read" "max_threads = 1, max_block_size = 7, remote_filesystem_read_method = 'read', $CACHE" "05303_warm_read"
Q+="
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMin(ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0, event_time_microseconds) AS first_query_read_source, sum(ProfileEvents['CachedReadBufferReadFromCacheBytes']) > 0 AS read_cache
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05303_cold', '05303_warm_threadpool', '05303_warm_read') GROUP BY log_comment ORDER BY log_comment;
"
run_batch

echo "-- requests: rows of eight kilobytes, a remote buffer of 4 KiB, one request per seek"
BIG_TYPE="JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))"
BIG_ROW="concat('{\"a\":', toString(number * 10), ',\"b\":\"', arrayStringConcat(arrayMap(i -> hex(sipHash128(toString(number * 1000 + i))), range(256))), '\",\"c\":', toString(range(number % 4)), '}')"
BIG_LAYOUT="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760, min_compress_block_size = 1, min_bytes_for_full_part_storage = 0, vertical_merge_algorithm_min_rows_to_activate = 1000000000"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS big;
CREATE TABLE big (x UInt64, json $BIG_TYPE) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_no_cache', $BIG_LAYOUT;
INSERT INTO big SELECT number, $BIG_ROW FROM numbers(120) SETTINGS max_threads = 1, max_insert_threads = 1;
"
# The object of the data file, to pick its requests out of the server logs, and its size.
object() { $CLICKHOUSE_CLIENT -q "SELECT remote_path FROM system.remote_data_paths WHERE local_path = (SELECT concat(substring(path, position(path, 'store/')), 'json.object_typed_shared_data.$2.bin') FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active) LIMIT 1"; }
object_size() { $CLICKHOUSE_CLIENT -q "SELECT size FROM system.remote_data_paths WHERE remote_path = '$1' LIMIT 1"; }
KEY=$(object big paths_data)
SIZE=$(object_size "$KEY")
S="max_threads = 1, remote_read_min_bytes_for_seek = 1, max_read_buffer_size_remote_fs = 4096, remote_filesystem_read_method = 'read', remote_filesystem_read_prefetch = 0, allow_prefetched_read_pool_for_remote_filesystem = 0, merge_tree_min_rows_for_concurrent_read = 1000000, merge_tree_min_bytes_for_concurrent_read = 1000000000, merge_tree_use_deserialization_prefixes_cache = 0, $NO_CACHE"
ranges() # object key, query -> the ranges requested from the object, one "begin end" per line
{
    $CLICKHOUSE_CLIENT --send_logs_level=test -q "$2 FORMAT Null" 2>&1 | grep "Read S3 object" | grep "Key: $1," | grep -o "Range: [0-9]*-[0-9]*" | sed -E 's/Range: ([0-9]+)-([0-9]+)/\1 \2/'
}
requests() { ranges "$1" "$2" | wc -l; }
bytes() { ranges "$1" "$2" | awk '{ s += $2 - $1 + 1 } END { print s + 0 }'; }
flag() { [ "$1" ] && echo "$2: 1" || echo "$2: 0 ($3)"; }
# The marks and the primary key are loaded once per part: a warm-up so that only data requests differ between queries.
$CLICKHOUSE_CLIENT -q "SELECT sum(json.a), sum(length(json.b)), sum(json.c.size0) FROM big FORMAT Null"
# A path read seeks to its path in every granule; the granules are far apart, so every seek is a request.
a_whole=$(requests "$KEY" "SELECT sum(json.a) FROM big SETTINGS $S")
a_block4=$(requests "$KEY" "SELECT sum(json.a) FROM big SETTINGS $S, max_block_size = 4")
flag "$a_whole -eq 30" "a path read, 30 granules, one request per granule" "$a_whole"
flag "$a_block4 -eq 30" "the same in calls of 4 rows" "$a_block4"
# The requests are not bounded, so their ranges run to the task's end: the counts tell the story, not the bytes.
b_8=$(requests "$KEY" "SELECT sum(length(json.b)) FROM big WHERE x < 8 SETTINGS $S")
flag "$b_8 -eq 2" "two granules, two requests" "$b_8"
two_ranges=$(requests "$KEY" "SELECT sum(length(json.b)) FROM big WHERE x < 8 OR (x >= 52 AND x < 56) SETTINGS $S")
flag "$two_ranges -eq 3" "two ranges of three granules, three requests" "$two_ranges"
c_size=$(requests "$KEY" "SELECT sum(json.c.size0) FROM big SETTINGS $S")
c_whole=$(requests "$KEY" "SELECT sum(length(json.c)) FROM big SETTINGS $S")
flag "$c_size -eq $c_whole" "a subcolumn issues the requests of its path" "$c_size vs $c_whole requests"
# The whole column reads the data in order: one request per task, covering the file once.
$CLICKHOUSE_CLIENT -q "SELECT 'whole column in calls of 5 rows', (SELECT arraySort(groupArray((x, toString(json)))) FROM big SETTINGS $S, max_block_size = 5) = (SELECT arraySort(groupArray((number, toString(CAST($BIG_ROW, '$BIG_TYPE'))))) FROM numbers(120))"
whole_requests=$(requests "$KEY" "SELECT json FROM big SETTINGS $S, max_block_size = 5")
whole_bytes=$(bytes "$KEY" "SELECT json FROM big SETTINGS $S, max_block_size = 5")
flag "$whole_requests -eq 1" "the whole column reads the data in one request" "$whole_requests"
flag "$whole_bytes -le $(( SIZE * 11 / 10 ))" "the whole column reads the data about once" "$whole_bytes vs $SIZE bytes"

echo "-- a merge of two parts in small blocks, and a one-row patch part of a lightweight update"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS big_merge;
CREATE TABLE big_merge (x UInt64, json $BIG_TYPE) ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 's3_no_cache', $BIG_LAYOUT, merge_max_block_size = 5, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES big_merge;
INSERT INTO big_merge SELECT number, $BIG_ROW FROM numbers(120) SETTINGS max_threads = 1, max_insert_threads = 1;
INSERT INTO big_merge SELECT number, $BIG_ROW FROM numbers(120, 120) SETTINGS max_threads = 1, max_insert_threads = 1;
SYSTEM START MERGES big_merge;
OPTIMIZE TABLE big_merge FINAL;
SELECT 'merged', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'big_merge' AND active;
SELECT 'merged rows', (SELECT arraySort(groupArray((x, toString(json)))) FROM big_merge SETTINGS max_threads = 1, max_block_size = 7) = (SELECT arraySort(groupArray((number, toString(CAST($BIG_ROW, '$BIG_TYPE'))))) FROM numbers(240));
UPDATE big_merge SET json = CAST('{\"a\":1,\"b\":\"upd\",\"c\":[7]}', '$BIG_TYPE') WHERE x = 7;
SELECT 'patch', json.a, json.b, json.c.size0 FROM big_merge WHERE x = 7 SETTINGS max_threads = 1, $NO_CACHE;
SELECT 'patched rows', (SELECT count() FROM big_merge WHERE json.b = 'upd' SETTINGS max_threads = 1, $NO_CACHE), (SELECT sum(json.a) FROM big_merge SETTINGS max_threads = 1, $NO_CACHE) = (SELECT sum(number * 10) - 69 FROM numbers(240));
"

echo "-- two parts read by many tasks: the prefetched read pool, tasks starting after mark 0, the cache cold and warm"
TASKS_LAYOUT="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 128, index_granularity_bytes = 10485760, min_bytes_for_full_part_storage = 0"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS mem_tasks; DROP TABLE IF EXISTS s3_tasks; DROP TABLE IF EXISTS s3c_tasks;
CREATE TABLE mem_tasks (x UInt64, json $PLAIN) ENGINE = Memory;
INSERT INTO mem_tasks SELECT number, $ROW FROM numbers(8000);
CREATE TABLE s3_tasks (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_no_cache', $TASKS_LAYOUT;
CREATE TABLE s3c_tasks (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_cache', $TASKS_LAYOUT;
SYSTEM STOP MERGES s3_tasks; SYSTEM STOP MERGES s3c_tasks;
INSERT INTO s3_tasks SELECT number, $ROW FROM numbers(4000) SETTINGS max_threads = 1, max_insert_threads = 1;
INSERT INTO s3_tasks SELECT number, $ROW FROM numbers(4000, 4000) SETTINGS max_threads = 1, max_insert_threads = 1;
INSERT INTO s3c_tasks SELECT number, $ROW FROM numbers(4000) SETTINGS max_threads = 1, max_insert_threads = 1, enable_filesystem_cache_on_write_operations = 0;
INSERT INTO s3c_tasks SELECT number, $ROW FROM numbers(4000, 4000) SETTINGS max_threads = 1, max_insert_threads = 1, enable_filesystem_cache_on_write_operations = 0;
SELECT table, count(), sum(rows), sum(marks) FROM system.parts WHERE database = currentDatabase() AND table IN ('s3_tasks', 's3c_tasks') AND active GROUP BY table ORDER BY table;
"
tasks() # table, label, query settings, [log comment]: EXCEPT in both directions and aggregates against the twin
{
    local table=$1 label=$2 settings=$3 tail=""
    [ -n "${4:-}" ] && tail=" SETTINGS log_comment = '$4'"
    Q+="
    SELECT '$label (json, json.a)', (SELECT count() FROM (SELECT x, toString(json), json.a FROM $table SETTINGS $settings EXCEPT SELECT x, toString(json), json.a FROM mem_tasks)), (SELECT count() FROM (SELECT x, toString(json), json.a FROM mem_tasks EXCEPT SELECT x, toString(json), json.a FROM $table SETTINGS $settings))$tail;
    SELECT '$label paths', (SELECT (sum(json.a), uniqExact(json.d.y), countIf(json.c.size0 = 2), sum(json.d.x), sum(length(json.d.y))) FROM $table SETTINGS $settings) = (SELECT (sum(json.a), uniqExact(json.d.y), countIf(json.c.size0 = 2), sum(json.d.x), sum(length(json.d.y))) FROM mem_tasks)$tail;
    SELECT '$label sub-object', (SELECT count() FROM (SELECT x, json.^d FROM $table SETTINGS $settings EXCEPT SELECT x, json.^d FROM mem_tasks))$tail;
    SELECT '$label scattered rows', (SELECT arraySort(groupArray((x, toString(json), json.a))) FROM $table WHERE x IN (3, 777, 2047, 2048, 3999, 4000, 4001, 6143, 7000, 7999) SETTINGS $settings) = (SELECT arraySort(groupArray((x, toString(json), json.a))) FROM mem_tasks WHERE x IN (3, 777, 2047, 2048, 3999, 4000, 4001, 6143, 7000, 7999))$tail;
    "
}
# The uncompressed cache is off, so that a warm run reads its bytes from the filesystem cache and not from memory.
T="max_threads = 16, merge_tree_min_rows_for_concurrent_read = 512, merge_tree_min_bytes_for_concurrent_read = 1, remote_filesystem_read_method = 'threadpool', allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_min_bytes_for_single_read_task = 1, filesystem_prefetch_step_marks = 0, filesystem_prefetch_step_bytes = 0, optimize_move_to_prewhere = 0, use_query_condition_cache = 0, use_uncompressed_cache = 0, use_columns_cache = 0"
for pool in 0 1; do
    tasks s3_tasks "s3_tasks, prefixes pool $pool" "$T, merge_tree_use_prefixes_deserialization_thread_pool = $pool, enable_filesystem_cache = 0, use_page_cache_for_disks_without_file_cache = 0"
done
tasks s3_tasks "s3_tasks, page cache" "$T, enable_filesystem_cache = 0, use_page_cache_for_disks_without_file_cache = 1"
tasks s3_tasks "s3_tasks, page cache warm" "$T, enable_filesystem_cache = 0, use_page_cache_for_disks_without_file_cache = 1"
tasks s3c_tasks "s3c_tasks cold" "$T, enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0" "05303_tasks_cold"
tasks s3c_tasks "s3c_tasks warm" "$T, enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0" "05303_tasks_warm"
Q+="
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, sum(ProfileEvents['CachedReadBufferReadFromCacheBytes']) > 0 AS read_cache
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05303_tasks_cold', '05303_tasks_warm') GROUP BY log_comment ORDER BY log_comment;
DROP TABLE mem; DROP TABLE s3; DROP TABLE s3p; DROP TABLE s3c; DROP TABLE big; DROP TABLE big_merge;
DROP TABLE mem_tasks; DROP TABLE s3_tasks; DROP TABLE s3c_tasks;
"
run_batch
