#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs S3 (MinIO) and Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 05243_query_condition_cache_top_k_parquet).

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Parquet files in object storage.
# The first run records the row groups that hold no row of the result, a rerun of the same query skips
# them. The entries depend on the running TopK threshold, which comes from the rows of every object the
# query reads, so they are keyed by the TopK plan, the predicate, and the paths and ETags of all the
# objects. Rewriting one object of a glob must invalidate the entries of the other, unchanged objects
# too - otherwise the rows of the unchanged object that now belong to the result would stay skipped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="${CLICKHOUSE_DATABASE}_05340"

# `a.parquet` holds the largest values, `b.parquet` smaller ones; each object keeps its largest values
# in its first row group. So all row groups but the first one of `a.parquet` hold no row of the result
# of `ORDER BY k DESC LIMIT 3`.
function write_object()
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/$1.parquet', format = Parquet)
        SELECT if(number < 1000, $2 + number, number % $3) AS k, toString(k) AS s FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
}
write_object a 1000000 1000
write_object b 100000 1000

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05340"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05340 (k Int64, s String) ENGINE = S3(s3_conn, filename = '${S3_DIR}/*.parquet', format = Parquet)"

SETTINGS="use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1"

function run()
{
    local name=$1
    local query=$2
    local extra_settings=${3:-}
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}" --query "$query SETTINGS ${SETTINGS}${extra_settings}"
}

# Per query: whether it consulted the cache at all, and whether it found entries there.
function events()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    for name in "$@"
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT '${name}',
                ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
                ProfileEvents['QueryConditionCacheHits'] > 0
            FROM system.query_log
            WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
    done
}

echo "--- no predicate"
run nowhere_1 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 3"
run nowhere_2 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 3"
# A different LIMIT is a different TopK plan: it must not reuse the entries.
run nowhere_limit "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 4"
# The TopK entries must not reach a plain read, nor a TopK read with the other direction.
run nowhere_plain "SELECT count(), sum(k) FROM t_05340"
run nowhere_asc "SELECT k FROM t_05340 ORDER BY k LIMIT 3"

echo "--- with a predicate"
run where_1 "SELECT k, s FROM t_05340 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_2 "SELECT k, s FROM t_05340 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run where_plain "SELECT count() FROM t_05340 WHERE k % 3 = 0"

echo "--- one object of the glob is rewritten"
# Now `a.parquet` holds the smallest values, and the result comes from the first row group of the
# unchanged `b.parquet`, which the entries above recorded as holding no row of the result.
write_object a 0 10
run rewritten_nowhere_1 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 3"
run rewritten_where_1 "SELECT k, s FROM t_05340 WHERE k % 3 = 0 ORDER BY k DESC LIMIT 3"
run rewritten_nowhere_2 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 3"

echo "--- reads not pinned to the listed ETag"
# An object could be overwritten between the listing and the read, so the key is not made.
run unpinned_1 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 5" ", s3_validate_etag_on_read = 0"
run unpinned_2 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 5" ", s3_validate_etag_on_read = 0"

echo "--- a predicate on the tags of the objects"
# The tags can change without changing the ETag of an object, so the key is not made.
run tags_1 "SELECT k FROM t_05340 WHERE _tags['x'] = '' ORDER BY k DESC LIMIT 7"
run tags_2 "SELECT k FROM t_05340 WHERE _tags['x'] = '' ORDER BY k DESC LIMIT 7"

echo "--- use_query_condition_cache_for_top_k = 0"
run gate_1 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 6" ", use_query_condition_cache_for_top_k = 0"
run gate_2 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 6" ", use_query_condition_cache_for_top_k = 0"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 nowhere_limit nowhere_plain nowhere_asc where_1 where_2 rewritten_nowhere_1 rewritten_where_1 rewritten_nowhere_2 unpinned_1 unpinned_2 tags_1 tags_2 gate_1 gate_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340"
