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
        SELECT if(number < 1000, $2 + number, number % $3) AS k, toString(k) AS s FROM numbers(20000)
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
    local ids=""
    for name in "$@"
    do
        ids+="${ids:+, }'${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}'"
    done
    ${CLICKHOUSE_CLIENT} --query "
        SELECT substring(query_id, length('${CLICKHOUSE_TEST_UNIQUE_NAME}_') + 1),
            ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses'] > 0,
            ProfileEvents['QueryConditionCacheHits'] > 0
        FROM system.query_log
        WHERE query_id IN (${ids}) AND current_database = currentDatabase() AND type = 'QueryFinish'
        ORDER BY indexOf([${ids}], query_id)"
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

echo "--- a predicate on the modification time of the objects"
# The modification time can change without changing the ETag of an object, so the key is not made.
run time_1 "SELECT k FROM t_05340 WHERE _time > toDateTime('2000-01-01 00:00:00', 'UTC') ORDER BY k DESC LIMIT 8"
run time_2 "SELECT k FROM t_05340 WHERE _time > toDateTime('2000-01-01 00:00:00', 'UTC') ORDER BY k DESC LIMIT 8"

echo "--- a predicate on a column with a read-time default"
# The objects lack `d`, so its value comes from the default expression, which the hash of the predicate
# does not cover: the first query lets all rows through, the second one only those with `k < 1000`.
# So the key is not made: the rerun finds no entries (it only looks up those of the predicate alone).
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05340_default"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05340_default (k Int64, s String, d String DEFAULT queryID()) ENGINE = S3(s3_conn, filename = '${S3_DIR}/*.parquet', format = Parquet)"
DEFAULT_QUERY="SELECT k FROM t_05340_default WHERE d = '${CLICKHOUSE_TEST_UNIQUE_NAME}_default_1' OR k < 1000 ORDER BY k DESC LIMIT 3"
DEFAULT_SETTINGS=", input_format_defaults_for_omitted_fields = 1, optimize_move_to_prewhere = 0"
run default_1 "$DEFAULT_QUERY" "$DEFAULT_SETTINGS"
run default_2 "$DEFAULT_QUERY" "$DEFAULT_SETTINGS"

echo "--- explicit keys"
# Listing several explicit keys ahead of the read would serialize a request for each of them, so a brace
# expansion makes no key. A single explicit key does.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05340_keys"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05340_keys (k Int64, s String) ENGINE = S3(s3_conn, filename = '${S3_DIR}/{a,b}.parquet', format = Parquet)"
run keys_1 "SELECT k FROM t_05340_keys ORDER BY k DESC LIMIT 3"
run keys_2 "SELECT k FROM t_05340_keys ORDER BY k DESC LIMIT 3"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05340_key"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05340_key (k Int64, s String) ENGINE = S3(s3_conn, filename = '${S3_DIR}/b.parquet', format = Parquet)"
run key_1 "SELECT k FROM t_05340_key ORDER BY k DESC LIMIT 3"
run key_2 "SELECT k FROM t_05340_key ORDER BY k DESC LIMIT 3"

echo "--- format settings changed by a reload of the table"
# `c.parquet` keeps the largest values under `K`, `d.parquet` smaller ones under `k`. With case-insensitive
# column matching, the result comes from `c.parquet`. After the table is attached again without it,
# `c.parquet` supplies defaults for `k`, and the result comes from `d.parquet`, whose row groups the
# entries of the first load recorded as holding no row of the result. The table keeps its UUID, so the
# key has to cover the format settings it captured.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}_case/c.parquet', format = Parquet)
    SELECT if(number < 1000, 1000000 + number, number % 1000) AS K FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}_case/d.parquet', format = Parquet)
    SELECT if(number < 1000, 100000 + number, number % 1000) AS k FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05340_case"
${CLICKHOUSE_CLIENT} --input_format_parquet_case_insensitive_column_matching 1 --query "
    CREATE TABLE t_05340_case (k Int64) ENGINE = S3(s3_conn, filename = '${S3_DIR}_case/*.parquet', format = Parquet)"
run case_1 "SELECT k FROM t_05340_case ORDER BY k DESC LIMIT 3"
run case_2 "SELECT k FROM t_05340_case ORDER BY k DESC LIMIT 3"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_05340_case"
${CLICKHOUSE_CLIENT} --input_format_parquet_case_insensitive_column_matching 0 --query "ATTACH TABLE t_05340_case"
run case_reloaded_1 "SELECT k FROM t_05340_case ORDER BY k DESC LIMIT 3"
run case_reloaded_2 "SELECT k FROM t_05340_case ORDER BY k DESC LIMIT 3"

echo "--- use_query_condition_cache_for_top_k = 0"
run gate_1 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 6" ", use_query_condition_cache_for_top_k = 0"
run gate_2 "SELECT k FROM t_05340 ORDER BY k DESC LIMIT 6" ", use_query_condition_cache_for_top_k = 0"

echo "--- query condition cache lookups"
events nowhere_1 nowhere_2 nowhere_limit nowhere_plain nowhere_asc where_1 where_2 rewritten_nowhere_1 rewritten_where_1 rewritten_nowhere_2 unpinned_1 unpinned_2 tags_1 tags_2 time_1 time_2 default_1 default_2 keys_1 keys_2 key_1 key_2 case_1 case_2 case_reloaded_1 case_reloaded_2 gate_1 gate_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340_default"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340_keys"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340_key"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05340_case"
