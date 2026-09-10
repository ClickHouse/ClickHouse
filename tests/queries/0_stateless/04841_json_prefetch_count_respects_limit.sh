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

# Both places that submit a prefetch are covered: a column's data substreams by every read below,
# and the dynamic prefixes by the JSON reads, which are the only ones with a prefix to deserialize.
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

# The per-query settings below intentionally override the values the test runner randomizes in:
# `--allow_repeated_settings` is in effect and the last occurrence wins. The page cache accounts no
# prefetch memory of its own and reading through the distributed cache replaces the buffer that owns
# the prefetch allocation, so both are pinned rather than left to the environment; the read pool is a
# per-query parameter here because the two pools reach the prefetch from different call paths.
# $1 - allow_prefetched_read_pool_for_remote_filesystem, $2 - filesystem_prefetches_limit,
# $3 - filesystem_prefetch_max_memory_usage, $4 - max_threads, $5 - log_comment suffix, $6 - query
run_query() {
    ${CLICKHOUSE_CLIENT} --query "$6" --log_comment "04841_$5_${CLICKHOUSE_TEST_UNIQUE_NAME}" \
        --allow_prefetched_read_pool_for_remote_filesystem "$1" \
        --filesystem_prefetches_limit "$2" --filesystem_prefetch_max_memory_usage "$3" \
        --remote_filesystem_read_prefetch 1 --remote_filesystem_read_method threadpool \
        --max_threads "$4" --merge_tree_prefetch_json_shared_data_substreams 1 \
        --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0 \
        --optimize_functions_to_subcolumns 0 --enable_filesystem_cache 0 \
        --use_uncompressed_cache 0 --use_page_cache_for_disks_without_file_cache 0 \
        --read_through_distributed_cache 0 > /dev/null
}

# $1 - log_comment suffix -> a scalar subquery yielding that query's prefetch count
count_of() {
    echo "(SELECT ProfileEvents['RemoteFSPrefetches'] FROM system.query_log
           WHERE current_database = currentDatabase()
             AND log_comment = '04841_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}'
             AND type = 'QueryFinish' AND event_date >= yesterday() AND is_initial_query
           ORDER BY event_time_microseconds DESC LIMIT 1)"
}

wide_read="SELECT * FROM t_wide150 FORMAT Null"
wide4_read="SELECT * FROM t_wide150_4parts FORMAT Null"
json_read="SELECT count() FROM t_json WHERE
           length(JSONAllPaths(jn1)) + length(JSONAllPaths(jn2)) + length(JSONAllPaths(jn3)) >= 0"

run_query 1 200 '1Gi'  4 'step_default_limit' "$wide4_read"
run_query 1 0   '10Gi' 4 'step_unlimited'     "$wide4_read"
run_query 0 200 '1Gi'  4 'plain_pool_limit'   "$wide_read"
run_query 0 0   '10Gi' 4 'plain_pool_unlim'   "$wide_read"
run_query 1 200 '1Gi'  8 'json_step_limit'    "$json_read"
run_query 1 0   '10Gi' 8 'json_step_unlim'    "$json_read"
run_query 1 0   1      4 'json_enc_bytes'     "SELECT count() FROM t_json_enc WHERE length(JSONAllPaths(jn)) >= 0"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "-- the whole read step stays at the default limit, over four concurrent readers"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_default_limit) = 200"
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

# Both JSON reads are the same fixture at the same max_threads, one at the default limit and one
# without a limit, so nothing here is compared against a literal: on a build that does not bound the
# read step the two submit the same number.
echo "-- a JSON read step at the default limit prefetches fewer substreams than an unbounded one"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_step_limit) < $(count_of json_step_unlim)"

echo "-- the memory bound alone stops prefetching, on an encrypted disk"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_enc_bytes) = 0"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE t_wide150; DROP TABLE t_wide150_4parts; DROP TABLE t_json; DROP TABLE t_json_enc"
