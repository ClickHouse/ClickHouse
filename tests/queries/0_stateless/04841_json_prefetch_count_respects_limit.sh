#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(33554432, None)
# - no-fasttest: uses object storage disks
# - no-parallel-replicas: other replicas read in their own read steps, with their own budgets

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_prefetches_limit` is a maximum number of filesystem prefetches and
# `filesystem_prefetch_max_memory_usage` a maximum for the memory they hold. The Wide reader
# allocates one read buffer per prefetched substream, and one JSON column expands into many
# substreams, so both bounds have to cover the substreams of a whole read step rather than the
# columns of one reader: the readers of one step are alive at the same time.

# Both bounds are on the buffers alive at once, while ProfileEvents counts submissions, so an exact
# count is only assertable where no stream is destroyed mid-query. That holds for these plain
# Nullable columns; it does not hold for a JSON column, whose prefix streams are released after
# deserialization, which is why the JSON counts below are compared against each other instead of
# against a fixed number.

# Both places that submit a prefetch are covered: a column's data substreams by every read below, and
# the dynamic prefixes by the two JSON rows (t_json and t_json_enc), the only reads here with a prefix
# to deserialize; their counts hold only while the prefix submissions are charged to the budget too.

# A stream keeps its reservation while it lives, because consuming a prefetch moves the allocation
# into the read buffer rather than freeing it, so one reservation covers however many times that
# stream is prefetched. t_batches is the read that observes this across several batches; the wide and
# JSON fixtures read one granule each, and so prefetch every one of their streams exactly once.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE t_wide150 ($(seq -f 'c%g Nullable(UInt64)' -s ', ' 1 150)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_w1/');

CREATE TABLE t_wide150_4parts ($(seq -f 'c%g Nullable(UInt64)' -s ', ' 1 150)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_w4/');

-- One part per reader and one granule per part, so every reader of the step is alive at once and
-- prefetches its substreams once: hence the pinned granularity, the 100-row inserts and STOP MERGES.
SYSTEM STOP MERGES t_wide150_4parts;

-- Plain UInt64, so one substream per column and every substream the same size: eight of them, five
-- granules of 8192 rows, and values a compression codec cannot shrink. A prefetch buffer smaller
-- than a granule's worth of data therefore leaves data pending after every batch, which is what
-- makes the reader prefetch the same streams again in the next one.
CREATE TABLE t_batches ($(seq -f 'c%g UInt64' -s ', ' 1 8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_b/');
SYSTEM STOP MERGES t_batches;

-- Packed part storage routes every file through ReadBufferFromFileView, which keeps the wrapped
-- buffer's state in itself between operations. On the default disk that buffer is an asynchronous
-- local-descriptor one, and the size it reports for a prefetch comes from exactly that state, so a
-- chain reading it without swapping the state back would charge zero bytes for a full-size buffer.
CREATE TABLE t_packed ($(seq -f 'c%g UInt64' -s ', ' 1 8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES t_packed;

CREATE TABLE t_json (jn1 Nullable(JSON), jn2 Nullable(JSON), jn3 Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_tj/');
SYSTEM STOP MERGES t_json;

-- The byte bound is measured on an encrypted disk on purpose: decryption wraps the buffer that owns
-- the prefetch allocation, so a chain that does not report its prefetch buffer size would read zero
-- bytes there and escape the bound entirely.
CREATE TABLE t_json_enc (jn Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         disk = disk(type = 'encrypted', key = '1234567812345678',
                     disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc/'));
"

# 150 Nullable columns are 300 prefetchable substreams (data plus null map per column), so one part
# already offers more substreams than the default limit and four parts offer four times as many.
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_wide150 SELECT $(seq -f 'number + %g' -s ', ' 1 150) FROM numbers(100)"
for _ in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_wide150_4parts SELECT $(seq -f 'number + %g' -s ', ' 1 150) FROM numbers(100)"
done

# Many distinct JSON path types expand Object -> Dynamic -> Variant into many substreams. What a read
# step holds at once is its concurrent readers times the substreams one reader keeps alive, and a JSON
# reader releases a prefix stream as soon as it is deserialized, so it never holds all of its own: the
# JSON reads below need eight concurrent readers to demand more than the default limit, which is eight
# parts read at max_threads 8. The encrypted table needs one column in one part, since its assertion
# is that nothing is prefetched at all.
json_value="toJSONString(map('a' || toString(number % 40),
        multiIf(number % 4 = 0, toString(number),
                number % 4 = 1, toString(number / 3),
                number % 4 = 2, toString(number % 7 = 0),
                toString(['x', 'y']))))"
for _ in $(seq 1 8); do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_json SELECT $json_value, $json_value, $json_value FROM numbers(100)"
done
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_json_enc SELECT $json_value FROM numbers(100)"

# sipHash64 rather than `number`: a sequential column compresses into a single buffer whatever its row
# count, and would then be prefetched once no matter how many granules it spans.
batched_values=$(seq -f 'sipHash64(number, %g)' -s ', ' 1 8)
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_batches SELECT $batched_values FROM numbers(40960)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_packed SELECT $batched_values FROM numbers(40960)"

# The per-query settings below intentionally override the values the test runner randomizes in:
# `--allow_repeated_settings` is in effect and the last occurrence wins. The page cache accounts no
# prefetch memory of its own and reading through the distributed cache replaces the buffer that owns
# the prefetch allocation, so both are pinned rather than left to the environment; the read pool is a
# per-query parameter here because the two pools reach the prefetch from different call paths.
# $1 - allow_prefetched_read_pool_for_remote_filesystem, $2 - filesystem_prefetches_limit,
# $3 - filesystem_prefetch_max_memory_usage, $4 - max_threads, $5 - log_comment suffix, $6 - query,
# $7... - extra settings for that one read
run_query() {
    local extra_settings=("${@:7}")
    ${CLICKHOUSE_CLIENT} --query "$6" --log_comment "04841_$5_${CLICKHOUSE_TEST_UNIQUE_NAME}" \
        --allow_prefetched_read_pool_for_remote_filesystem "$1" \
        --filesystem_prefetches_limit "$2" --filesystem_prefetch_max_memory_usage "$3" \
        --remote_filesystem_read_prefetch 1 --remote_filesystem_read_method threadpool \
        --max_threads "$4" --merge_tree_prefetch_json_shared_data_substreams 1 \
        --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0 \
        --optimize_functions_to_subcolumns 0 --enable_filesystem_cache 0 \
        --use_uncompressed_cache 0 --use_page_cache_for_disks_without_file_cache 0 \
        --read_through_distributed_cache 0 "${extra_settings[@]}" > /dev/null
}

# $1 - log_comment suffix -> a scalar subquery yielding that query's prefetch count
count_of() {
    echo "(SELECT ProfileEvents['RemoteFSPrefetches'] FROM system.query_log
           WHERE current_database = currentDatabase()
             AND log_comment = '04841_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}'
             AND type = 'QueryFinish' AND event_date >= yesterday() AND is_initial_query
           ORDER BY event_time_microseconds DESC LIMIT 1)"
}

# Local-descriptor prefetches increment no ProfileEvents counter, so the packed read below is counted
# from the prefetches log instead.
# $1 - log_comment suffix -> a scalar subquery yielding that query's logged prefetch count
logged_count_of() {
    echo "(SELECT count() FROM system.filesystem_read_prefetches_log WHERE query_id = (
               SELECT query_id FROM system.query_log
               WHERE current_database = currentDatabase()
                 AND log_comment = '04841_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}'
                 AND type = 'QueryFinish' AND event_date >= yesterday() AND is_initial_query
               ORDER BY event_time_microseconds DESC LIMIT 1))"
}

wide_read="SELECT * FROM t_wide150 FORMAT Null"
wide4_read="SELECT * FROM t_wide150_4parts FORMAT Null"
json_read="SELECT count() FROM t_json WHERE
           length(JSONAllPaths(jn1)) + length(JSONAllPaths(jn2)) + length(JSONAllPaths(jn3)) >= 0"

run_query 1 200 '1Gi'  4 'step_default_limit' "$wide4_read"
run_query 1 0   '10Gi' 4 'step_unlimited'     "$wide4_read"
run_query 0 200 '1Gi'  4 'plain_pool_limit'   "$wide_read"
run_query 0 0   '10Gi' 4 'plain_pool_unlim'   "$wide_read"
run_query 1 50  '1Gi'  8 'json_step_limit'    "$json_read"
run_query 1 0   '10Gi' 8 'json_step_unlim'    "$json_read"
run_query 1 0   1      4 'json_enc_bytes'     "SELECT count() FROM t_json_enc WHERE length(JSONAllPaths(jn)) >= 0"

# One part read by one thread, and a prefetch buffer well under a granule's worth of data so the
# reader still has data pending when the next batch starts.
batched_read="SELECT * FROM t_batches FORMAT Null"
run_query 0 4 '10Gi' 1 'batched_limit' "$batched_read" --max_read_buffer_size_remote_fs 4096
run_query 0 0 '10Gi' 1 'batched_unlim' "$batched_read" --max_read_buffer_size_remote_fs 4096

# The packed read is on the default disk, so the prefetches are local-descriptor ones: hence the local
# read method and the log, and hence remote prefetching off, so that a run whose default disk is object
# storage submits nothing rather than a prefetch this row cannot account for.
run_query 0 0 1 1 'packed_bytes' "SELECT * FROM t_packed FORMAT Null" \
    --local_filesystem_read_prefetch 1 --local_filesystem_read_method pread_threadpool \
    --remote_filesystem_read_prefetch 0 --enable_filesystem_read_prefetches_log 1 \
    --max_read_buffer_size_local_fs 4096
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log, filesystem_read_prefetches_log"

# The readers of one step are created as threads pick their tasks up, so two of them can charge the
# budget, finish, release it and let the next two charge it again. The cumulative submissions are then
# a multiple of the limit rather than the limit, which is why this row asserts the multiple: what the
# bound promises is that no set of readers alive together exceeds it.
echo "-- no set of readers alive together in the step exceeds the default limit"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_default_limit) % 200 = 0
                              AND $(count_of step_default_limit) > 0"
echo "-- and the step never reaches what its four readers asked for"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_default_limit) < $(count_of step_unlimited)"
echo "-- the limit is observed, not naturally small: four readers want 4 x 300 of them"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_unlimited) = 1200"

# The pool that prefetches on admission is off for these two, so the reads go through the reader's
# own path instead. One part on purpose: readers there are created as threads pick tasks up, so with
# several parts one pair can charge the budget, finish, release it and let the next pair charge it
# again, which makes the cumulative count a multiple of the limit rather than the limit.
echo "-- a single reader above the limit prefetches the first N substreams, not none"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of plain_pool_limit) = 200"
echo "-- and all 300 of them when unlimited"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of plain_pool_unlim) = 300"

# Both JSON reads are the same fixture at the same max_threads, one bounded and one not, so nothing
# here is compared against a literal: on a build that does not bound the read step the two submit the
# same number. The bound is well under the default one because releasing a prefix stream returns its
# capacity to the budget, so the bounded count grows with the query's churn rather than staying at the
# limit: at 200 it was measured within 6% of the unbounded count, at 50 it stays around a fifth of it.
echo "-- a bounded JSON read step prefetches fewer substreams than an unbounded one"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_step_limit) < $(count_of json_step_unlim)"

echo "-- the memory bound alone stops prefetching, on an encrypted disk"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_enc_bytes) = 0"

# Four of the eight streams get a reservation and keep it, so the bounded read submits four prefetches
# per batch where the unbounded one submits eight: the two counts divided by 4 and by 8 are the same
# batch count, cross-multiplied here to stay integral. A saturated budget that stopped the reader after
# the first batch would leave the bounded count at 4 whatever the second count is. How many batches a
# granule's data takes is not fixed (compression block sizes are randomized), which is why neither
# count is compared against a literal; the second row pins that there was more than one batch, since 8
# would mean the fixture collapsed into one and both counts would agree for the wrong reason.
echo "-- a reserved stream may prefetch again in a later batch, without new memory"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of batched_limit) * 8 = $(count_of batched_unlim) * 4"
echo "-- and it took more than one batch: unbounded, all eight streams prefetch in each of them"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of batched_unlim) > 8"

echo "-- the fixture is packed part storage"
${CLICKHOUSE_CLIENT} --query "SELECT any(part_storage_type) = 'Packed' FROM system.parts
                              WHERE database = currentDatabase() AND table = 't_packed' AND active"
echo "-- and the memory bound stops prefetching there too, through the file view"
${CLICKHOUSE_CLIENT} --query "SELECT $(logged_count_of packed_bytes) = 0"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE t_wide150; DROP TABLE t_wide150_4parts; DROP TABLE t_json; DROP TABLE t_json_enc;
DROP TABLE t_batches; DROP TABLE t_packed"
