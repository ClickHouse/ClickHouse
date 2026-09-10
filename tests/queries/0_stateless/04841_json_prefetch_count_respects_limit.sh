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
# deserialization, which is why the JSON count below is compared against the same fixture read
# without a limit instead of against a fixed number.
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

CREATE TABLE t_json (jn Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple()
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

# Many distinct JSON path types expand Object -> Dynamic -> Variant into many substreams. t_json
# gets four parts so that its read step wants more prefetches than the limit asserted on it; the
# encrypted table needs only one, since its assertion is that nothing is prefetched at all.
json_insert="SELECT toJSONString(map('a' || toString(number % 40),
        multiIf(number % 4 = 0, toString(number),
                number % 4 = 1, toString(number / 3),
                number % 4 = 2, toString(number % 7 = 0),
                toString(['x', 'y'])))) FROM numbers(100)"
for _ in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_json $json_insert"
done
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_json_enc $json_insert"

# The per-query settings below intentionally override the values the test runner randomizes in:
# `--allow_repeated_settings` is in effect and the last occurrence wins. The CI profile turns the
# prefetched read pool off, the page cache accounts no prefetch memory of its own, and reading
# through the distributed cache replaces the buffer that owns the prefetch allocation, so all three
# are pinned rather than left to the environment.
# $1 - filesystem_prefetches_limit, $2 - filesystem_prefetch_max_memory_usage,
# $3 - log_comment suffix, $4 - query
run_query() {
    ${CLICKHOUSE_CLIENT} --query "$4" --log_comment "04841_$3_${CLICKHOUSE_TEST_UNIQUE_NAME}" \
        --filesystem_prefetches_limit "$1" --filesystem_prefetch_max_memory_usage "$2" \
        --allow_prefetched_read_pool_for_remote_filesystem 1 \
        --remote_filesystem_read_prefetch 1 --remote_filesystem_read_method threadpool \
        --max_threads 4 --merge_tree_prefetch_json_shared_data_substreams 1 \
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
json_read="SELECT count() FROM t_json WHERE length(JSONAllPaths(jn)) >= 0"

run_query 200 '1Gi'  'step_default_limit' "$wide4_read"
run_query 0   '10Gi' 'step_unlimited'     "$wide4_read"
run_query 200 '1Gi'  'one_reader_limit'   "$wide_read"
run_query 0   '10Gi' 'one_reader_unlim'   "$wide_read"
run_query 50  '10Gi' 'json_limit'         "$json_read"
run_query 0   '10Gi' 'json_unlimited'     "$json_read"
run_query 0   1      'json_enc_bytes'     "SELECT count() FROM t_json_enc WHERE length(JSONAllPaths(jn)) >= 0"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "-- the whole read step stays at the default limit, over four concurrent readers"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_default_limit) = 200"
echo "-- the limit is observed, not naturally small: four readers want 4 x 300 of them"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of step_unlimited) = 1200"

echo "-- a single reader above the limit prefetches the first N substreams, not none"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of one_reader_limit) = 200"
echo "-- and all 300 of them when unlimited"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of one_reader_unlim) = 300"

echo "-- a JSON read step prefetches fewer substreams under a limit than without one"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_limit) < $(count_of json_unlimited)"

echo "-- the memory bound alone stops prefetching, on an encrypted disk"
${CLICKHOUSE_CLIENT} --query "SELECT $(count_of json_enc_bytes) = 0"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE t_wide150; DROP TABLE t_wide150_4parts; DROP TABLE t_json; DROP TABLE t_json_enc"
