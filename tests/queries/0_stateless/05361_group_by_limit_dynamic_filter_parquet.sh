#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: Parquet and S3
# - no-parallel-replicas: reads the profile events of the local read
# Random settings limits: use_skip_indexes_on_data_read=(1, 1)

# The top-K heap of GROUP BY ... LIMIT filters the rows of a Parquet read and skips its row groups by the heap
# boundary (enable_group_by_top_k_dynamic_filtering). Every result below is the one without the boundary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_DATABASE}_05361"
mkdir -p "${USER_FILES_PATH:?}/${DIR}"
trap 'rm -rf "${USER_FILES_PATH:?}/${DIR}"' EXIT

# One reading thread, so that the first row groups read set the boundary for the others.
SET_ALL="
SET enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 1;
SET max_rows_to_group_by = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0, use_query_condition_cache = 0;
SET optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SET allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 1;
SET max_threads = 1, max_parsing_threads = 1;
SET engine_file_truncate_on_insert = 1, s3_truncate_on_insert = 1;
"

# The blocks of every INSERT are as large as its row groups, which then hold exactly that many rows.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
${SET_ALL}
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}_count_1.parquet', format = Parquet) SELECT number AS k FROM numbers(10000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}_count_2.parquet', format = Parquet) SELECT 10000 + number AS k FROM numbers(10000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file('${DIR}/main.parquet', Parquet)
SELECT intDiv(number, 3) AS a, toString(number % 7) AS b, number AS c, intHash64(number) % 1000 AS r,
    if(number % 5 = 0, NULL, intDiv(number, 2) % 101) AS n
FROM numbers(100000) SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file('${DIR}/glob_0.parquet', Parquet) SELECT intDiv(number, 2) AS a FROM numbers(10000) WHERE number % 2 = 0
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file('${DIR}/glob_1.parquet', Parquet) SELECT intDiv(number, 2) AS a FROM numbers(10000) WHERE number % 2 = 1
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file('${DIR}/random.parquet', Parquet) SELECT intHash64(number) % 20000 AS a, number AS c FROM numbers(20000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file('${DIR}/json_0.parquet', Parquet) SELECT toJSONString(map('a', number + 1000)) AS j FROM numbers(2000) WHERE number % 2 = 0
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file('${DIR}/json_1.parquet', Parquet) SELECT toJSONString(map('a', number + 1000)) AS j FROM numbers(2000) WHERE number % 2 = 1
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file('${DIR}/array_json_0.parquet', Parquet) SELECT [toJSONString(map('a', number + 1000))] AS j FROM numbers(2000) WHERE number % 2 = 0
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file('${DIR}/array_json_1.parquet', Parquet) SELECT [toJSONString(map('a', number + 1000))] AS j FROM numbers(2000) WHERE number % 2 = 1
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

-- Row group 0 holds (1, 5) and (1000000, NULL), row group 1 holds (0, 0), (1, NULL), (1000000, 7) and (1000000, 3).
INSERT INTO FUNCTION file('${DIR}/tuple.parquet', Parquet) SELECT CAST(multiIf(
    number = 0, (1, 5), number = 1, (1000000, NULL), number < 1000, (100 + number, number),
    number = 1000, (0, 0), number = 1001, (1, NULL), number = 1002, (1000000, 7), number = 1003, (1000000, 3),
    (200 + number, number)), 'Tuple(a UInt64, b Nullable(UInt64))') AS s
FROM numbers(2000) SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file('${DIR}/string.parquet', Parquet) SELECT toString(number % 100 + 1000) AS s FROM numbers(10000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

-- Row group i holds a = i, in which b has 10 values and c 2.
INSERT INTO FUNCTION file('${DIR}/cache.parquet', Parquet) SELECT intDiv(number, 100) AS a, number % 10 AS b, number % 2 AS c FROM numbers(1000)
SETTINGS max_block_size = 100, output_format_parquet_row_group_size = 100;

INSERT INTO FUNCTION file('${DIR}/low.parquet', Parquet) SELECT if(number % 1000 = 7, 1 + number % 3, 0) AS k, number AS c FROM numbers(300000)
SETTINGS max_block_size = 10000, output_format_parquet_row_group_size = 10000;

-- Ten row groups for each value of a.
INSERT INTO FUNCTION file('${DIR}/steps.parquet', Parquet) SELECT intDiv(number, 1000) AS a, number AS c FROM numbers(20000)
SETTINGS max_block_size = 100, output_format_parquet_row_group_size = 100;

-- Row group 0 holds k = 0..9 and sets the boundary 9, the next 49 hold k = 1..8, and the last one holds one row
-- with k = 9 and larger values.
INSERT INTO FUNCTION file('${DIR}/tie.parquet', Parquet)
SELECT multiIf(number < 1000, number % 10, number < 50000, 1 + number % 8, number = 50000, 9, number) AS k FROM numbers(51000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

-- The first file sets the boundary, and every row group of the second one is beyond it.
INSERT INTO FUNCTION file('${DIR}/count_1.parquet', Parquet) SELECT number AS k FROM numbers(10000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file('${DIR}/count_2.parquet', Parquet) SELECT 10000 + number AS k FROM numbers(10000)
SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000;

-- Row group or granule i holds a = i, in which b has 10 values.
INSERT INTO FUNCTION file('${DIR}/stats.parquet', Parquet) SELECT intDiv(number, 100) AS a, number % 10 AS b, number % 2 AS c FROM numbers(1000)
SETTINGS max_block_size = 100, output_format_parquet_row_group_size = 100;
CREATE TABLE t_05361_stats (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
INSERT INTO t_05361_stats SELECT intDiv(number, 100), number % 10, number % 2 FROM numbers(100000);
CREATE TABLE t_05361_few (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05361_few SELECT number % 3 FROM numbers(100000);
CREATE TABLE t_05361_seed (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_05361_seed SELECT number % 3 FROM numbers(100000);

CREATE TABLE t_05361_random (a UInt64, c UInt64) ENGINE = File(Parquet, '${DIR}/random.parquet');
CREATE TABLE t_05361_tuple (s Tuple(a UInt64, b Nullable(UInt64))) ENGINE = File(Parquet, '${DIR}/tuple.parquet');
CREATE TABLE t_05361_tuple_nullable (s Nullable(Tuple(a UInt64, b Nullable(UInt64)))) ENGINE = File(Parquet, '${DIR}/tuple.parquet');
CREATE TABLE t_05361_cache (a UInt64, b UInt64, c UInt64) ENGINE = File(Parquet, '${DIR}/cache.parquet');
CREATE TABLE t_05361_steps (a UInt64, c UInt64) ENGINE = File(Parquet, '${DIR}/steps.parquet');
EOF

# The query condition cache and the row count cache use a file only once its last modification is in the past.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DIR}"/*.parquet
sleep 1

${CLICKHOUSE_CLIENT} --multiquery <<EOF
${SET_ALL}
SELECT 'results';
SELECT a, b, count(), sum(c) FROM file('${DIR}/main.parquet') GROUP BY a, b LIMIT 10;
SELECT a, count(), max(c) FROM file('${DIR}/main.parquet') GROUP BY a ORDER BY a DESC LIMIT 5;
SELECT r, count(), min(c) FROM file('${DIR}/main.parquet') GROUP BY r LIMIT 7;
SELECT n, count() FROM file('${DIR}/main.parquet') GROUP BY n ORDER BY n NULLS FIRST LIMIT 3;
SELECT n, count() FROM file('${DIR}/main.parquet') GROUP BY n ORDER BY n DESC NULLS LAST LIMIT 3;
SELECT a, count() FROM file('${DIR}/main.parquet') WHERE b != '3' GROUP BY a LIMIT 4;
SELECT a, count() FROM file('${DIR}/glob_{0,1}.parquet') GROUP BY a LIMIT 5;
SELECT a, count(), sum(c) FROM remote('127.0.0.{1,2}', currentDatabase(), t_05361_random) GROUP BY a ORDER BY a LIMIT 5;

SELECT 'a row group that starts at the boundary';
SELECT k, count() FROM file('${DIR}/tie.parquet') GROUP BY k LIMIT 10;

SELECT 'the boundary filters rows and skips row groups';
SELECT a, count() FROM file('${DIR}/main.parquet') GROUP BY a LIMIT 3 FORMAT Null SETTINGS log_comment = '05361_on';
SELECT a, count() FROM file('${DIR}/main.parquet') GROUP BY a LIMIT 3 FORMAT Null
SETTINGS log_comment = '05361_off', enable_group_by_top_k_dynamic_filtering = 0;

SELECT 'keys the boundary cannot be used for';
SELECT j, count() FROM file('${DIR}/json_{0,1}.parquet', Parquet, 'j JSON') GROUP BY j ORDER BY j LIMIT 3
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1;
SELECT j, count() FROM file('${DIR}/array_json_{0,1}.parquet', Parquet, 'j Array(JSON)') GROUP BY j ORDER BY j LIMIT 3
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1;
SELECT s, count() FROM t_05361_tuple GROUP BY s ORDER BY s LIMIT 3 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple GROUP BY s ORDER BY s LIMIT 3 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple GROUP BY s ORDER BY s DESC NULLS FIRST LIMIT 2 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple GROUP BY s ORDER BY s DESC NULLS FIRST LIMIT 2 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple_nullable GROUP BY s ORDER BY s LIMIT 3 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple_nullable GROUP BY s ORDER BY s LIMIT 3 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple_nullable GROUP BY s ORDER BY s DESC NULLS FIRST LIMIT 2 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM t_05361_tuple_nullable GROUP BY s ORDER BY s DESC NULLS FIRST LIMIT 2 SETTINGS use_query_condition_cache = 1;
SELECT s, count() FROM file('${DIR}/string.parquet') GROUP BY s LIMIT 3 SETTINGS log_comment = '05361_string_off';
SELECT s, count() FROM file('${DIR}/string.parquet') GROUP BY s LIMIT 3
SETTINGS log_comment = '05361_string_on', use_top_k_dynamic_filtering_for_variable_length_types = 1;

SELECT 'a PREWHERE or row policy that depends on its block';
SELECT r, count() FROM file('${DIR}/main.parquet') PREWHERE rowNumberInBlock() % 2 = 0 GROUP BY r ORDER BY r LIMIT 5;
SELECT r, count() FROM file('${DIR}/main.parquet') PREWHERE arrayExists(y -> rowNumberInBlock() % 2 = y, [0]) GROUP BY r ORDER BY r DESC LIMIT 5;
CREATE ROW POLICY p_05361 ON t_05361_random USING arrayExists(y -> rowNumberInBlock() % 3 = y, [0]) TO ALL;
SELECT a, count() FROM t_05361_random GROUP BY a ORDER BY a LIMIT 5;
DROP ROW POLICY p_05361 ON t_05361_random;

SELECT 'the query condition cache';
SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1;
SELECT count(), sum(x), max(a) FROM (SELECT a, b, count() AS x FROM t_05361_cache GROUP BY a, b LIMIT 10);
SELECT count(), sum(x), max(a) FROM (SELECT a, c, count() AS x FROM t_05361_cache GROUP BY a, c LIMIT 10);
SELECT count(), sum(x), max(a) FROM (SELECT a, b, count() AS x FROM t_05361_cache GROUP BY a, b LIMIT 10);
SET use_query_condition_cache = 0;

SELECT 'the boundary stops filtering when it drops almost nothing';
SELECT k, count() FROM file('${DIR}/low.parquet') GROUP BY k ORDER BY k LIMIT 2 SETTINGS log_comment = '05361_low';
SELECT k, count() FROM file('${DIR}/low.parquet') PREWHERE c % 1000 != 999 GROUP BY k ORDER BY k LIMIT 2 SETTINGS log_comment = '05361_low_prewhere';
SELECT k, count() FROM file('${DIR}/low.parquet') GROUP BY k ORDER BY k DESC LIMIT 2;
-- ORDER BY ... LIMIT does not stop.
SELECT k FROM file('${DIR}/low.parquet') ORDER BY k LIMIT 10 FORMAT Null SETTINGS log_comment = '05361_low_order';
-- The PREWHERE keeps whole row groups, but statistics cannot tell which. The pages read are compared, so not from the columns cache.
SELECT k, count() FROM file('${DIR}/low.parquet') PREWHERE cityHash64(intDiv(c, 10000)) % 10 = 0 GROUP BY k ORDER BY k LIMIT 2
SETTINGS log_comment = '05361_low_hash_on', use_columns_cache = 0;
SELECT k, count() FROM file('${DIR}/low.parquet') PREWHERE cityHash64(intDiv(c, 10000)) % 10 = 0 GROUP BY k ORDER BY k LIMIT 2
FORMAT Null SETTINGS log_comment = '05361_low_hash_off', enable_group_by_top_k_dynamic_filtering = 0, use_columns_cache = 0;

SELECT 'a row policy and a PREWHERE after the boundary';
CREATE ROW POLICY p_05361_steps ON t_05361_steps USING c % 3 != 1 TO ALL;
SELECT a, count(), sum(c) FROM t_05361_steps PREWHERE c % 5 != 2 GROUP BY a LIMIT 5 SETTINGS log_comment = '05361_steps';
DROP ROW POLICY p_05361_steps ON t_05361_steps;

SELECT 'the row count of a file is not cached from a read with the boundary';
SELECT k, count() FROM file('${DIR}/count_{1,2}.parquet') GROUP BY k LIMIT 3;
SELECT _file, count() FROM file('${DIR}/count_{1,2}.parquet') GROUP BY _file ORDER BY _file;
SELECT k, count() FROM s3(s3_conn, filename = '${DIR}_count_{1,2}.parquet', format = Parquet) GROUP BY k LIMIT 3 SETTINGS log_comment = '05361_count_s3';
SELECT replaceOne(_file, '${DIR}', '') AS f, count() FROM s3(s3_conn, filename = '${DIR}_count_{1,2}.parquet', format = Parquet)
GROUP BY f ORDER BY f;

SELECT 'a rerun keeps the boundary';
SELECT 'h1', groupArray((a, b, x)) FROM (SELECT a, b, count() AS x FROM file('${DIR}/stats.parquet') GROUP BY a, b LIMIT 10)
SETTINGS log_comment = '05361_stats_pq_1', collect_hash_table_stats_during_aggregation = 1;
SELECT 'h1', groupArray((a, b, x)) FROM (SELECT a, b, count() AS x FROM file('${DIR}/stats.parquet') GROUP BY a, b LIMIT 10)
SETTINGS log_comment = '05361_stats_pq_2', collect_hash_table_stats_during_aggregation = 1;
SELECT 'h2', groupArray((a, b, x)) FROM (SELECT a, b, count() AS x FROM t_05361_stats GROUP BY a, b LIMIT 10)
SETTINGS log_comment = '05361_stats_mt_1', max_block_size = 100, optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 1;
SELECT 'h2', groupArray((a, b, x)) FROM (SELECT a, b, count() AS x FROM t_05361_stats GROUP BY a, b LIMIT 10)
SETTINGS log_comment = '05361_stats_mt_2', max_block_size = 100, optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 1;

SELECT 'a run whose threads see the same few groups records them';
SELECT k, count() FROM t_05361_few GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05361_few_1', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    collect_hash_table_stats_during_aggregation = 1, log_processors_profiles = 1;
SELECT k, count() FROM t_05361_few GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05361_few_2', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    collect_hash_table_stats_during_aggregation = 1, log_processors_profiles = 1;

SELECT 'a linked run without a boundary keeps a larger record';
SELECT k, count() FROM t_05361_seed GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05361_seed_1', max_threads = 16, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    collect_hash_table_stats_during_aggregation = 1, log_processors_profiles = 1;
SELECT k, count() FROM t_05361_seed GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05361_seed_2', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    collect_hash_table_stats_during_aggregation = 1, log_processors_profiles = 1;
SELECT k, count() FROM t_05361_seed GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05361_seed_3', max_threads = 2, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    collect_hash_table_stats_during_aggregation = 1, log_processors_profiles = 1;

SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT log_comment, ProfileEvents['ParquetRowsFilterExpression'] > 0, ProfileEvents['ParquetTopKSkippedRowGroups'] > 0,
    ProfileEvents['SelectedRows'] < 100000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05361_on', '05361_off') AND type = 'QueryFinish'
ORDER BY log_comment;
SELECT log_comment, ProfileEvents['ParquetRowsFilterExpression'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05361_string_off', '05361_string_on') AND type = 'QueryFinish'
ORDER BY log_comment;
-- The threshold step ran and then stopped; the PREWHERE alone filters 300000 rows of low.parquet.
SELECT log_comment, multiIf(
    log_comment = '05361_low', ProfileEvents['ParquetRowsFilterExpression'] BETWEEN 1 AND 99999,
    log_comment = '05361_low_prewhere', ProfileEvents['ParquetRowsFilterExpression'] BETWEEN 300001 AND 449999,
    ProfileEvents['ParquetRowsFilterExpression'] > 200000)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05361_low', '05361_low_order', '05361_low_prewhere') AND type = 'QueryFinish'
ORDER BY log_comment;
-- Once the threshold step stopped, k is read only in the row groups the PREWHERE keeps, as without the boundary.
SELECT 'low_hash', sumIf(ProfileEvents['ParquetReadPages'], log_comment = '05361_low_hash_on')
    < 1.25 * sumIf(ProfileEvents['ParquetReadPages'], log_comment = '05361_low_hash_off')
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05361_low_hash_on', '05361_low_hash_off') AND type = 'QueryFinish';
SELECT log_comment, ProfileEvents['SelectedRows'] < if(log_comment LIKE '05361_stats_pq_%', 1000, 100000)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '05361_stats_%' AND type = 'QueryFinish'
ORDER BY log_comment;
-- The object storage read and the read under a row policy are linked too.
SELECT log_comment, ProfileEvents['ParquetTopKSkippedRowGroups'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05361_count_s3', '05361_steps') AND type = 'QueryFinish'
ORDER BY log_comment;
-- Two threads hold the 3 groups each, and 6 <= 5 * 1.5 drops the heap and its sort from the second run.
SELECT 'few_1', countDistinct(name) > 0 FROM system.processors_profile_log
WHERE event_date >= yesterday() AND name LIKE '%Sorting%' AND query_id IN (SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05361_few_1' AND type = 'QueryFinish');
SELECT 'few_2', countDistinct(name) > 0 FROM system.processors_profile_log
WHERE event_date >= yesterday() AND name LIKE '%Sorting%' AND query_id IN (SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05361_few_2' AND type = 'QueryFinish');
-- The first run records 3 groups per thread that read rows (its AggregatingTransform takes in more rows than it passes on),
-- and the heap stays while the record is above 5 * 1.5. The second, linked and without a boundary, must not lower it to 6
-- when both its threads read rows; with one, it records the true 3.
SELECT 'seed_3', (countIf(q.log_comment = '05361_seed_3' AND p.name LIKE '%Sorting%') > 0)
    = (countIf(q.log_comment = '05361_seed_1' AND p.name = 'AggregatingTransform' AND p.input_rows > p.output_rows) >= 3
        AND countIf(q.log_comment = '05361_seed_2' AND p.name = 'AggregatingTransform' AND p.input_rows > p.output_rows) = 2)
FROM system.processors_profile_log AS p
INNER JOIN (SELECT query_id, log_comment FROM system.query_log WHERE event_date >= yesterday()
    AND current_database = currentDatabase() AND log_comment LIKE '05361_seed_%' AND type = 'QueryFinish') AS q USING (query_id)
WHERE p.event_date >= yesterday();

DROP TABLE t_05361_few;
DROP TABLE t_05361_seed;
DROP TABLE t_05361_stats;
DROP TABLE t_05361_random;
DROP TABLE t_05361_tuple;
DROP TABLE t_05361_tuple_nullable;
DROP TABLE t_05361_cache;
DROP TABLE t_05361_steps;
EOF

# A GeoParquet read with spatial pruning has statistics-only columns besides the ones it returns.
$CLICKHOUSE_LOCAL -q "
SELECT id, count() FROM file('$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
GROUP BY id ORDER BY id LIMIT 1
SETTINGS enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1,
    query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
    input_format_parquet_spatial_filter_push_down = 1, max_threads = 1, max_parsing_threads = 1"
