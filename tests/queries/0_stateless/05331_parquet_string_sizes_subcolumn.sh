#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Parquet, MinIO, IcebergLocal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `length(s)`, `empty(s)`, `notEmpty(s)` and `s != ''` over Parquet read only the string lengths, as `s.size`.
# Results are compared with `optimize_functions_to_subcolumns = 0`, which reads the strings.

# Written by pyarrow: `s` DELTA_BYTE_ARRAY, `l` DELTA_LENGTH_BYTE_ARRAY, `o` OPTIONAL with nulls,
# `f` dictionary-encoded pages followed by PLAIN pages in the same column chunk.
cp "$CUR_DIR/data_parquet/05331_parquet_string_sizes.parquet" "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_05331_fixture.parquet"
# A DELTA_BYTE_ARRAY Decimal.
cp "$CUR_DIR/data_parquet/05035_delta_byte_array_decimal.parquet" "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_05331_decimal.parquet"

# Pinned against randomization. With `join_use_nulls = 1` the pass is disabled for queries with joins.
SETTINGS="SET optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, join_use_nulls = 0;"

{ echo "${SETTINGS}"; cat; } <<'EOF' | ${CLICKHOUSE_CLIENT}
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05331_plain.parquet', Parquet)
SELECT toUInt32(number) AS n, if(number % 4 = 0, '', concat('https://example.com/', toString(number * 7919))) AS s
FROM numbers(20000) SETTINGS output_format_parquet_max_dictionary_size = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05331_dict.parquet', Parquet)
SELECT toUInt32(number) AS n, if(number % 4 = 0, '', concat('v_', toString(number % 50))) AS s
FROM numbers(20000) SETTINGS output_format_parquet_max_dictionary_size = 1048576;

INSERT INTO FUNCTION file(currentDatabase() || '_05331_pages.parquet', Parquet)
SELECT toUInt32(number) AS n, if(number % 4 = 0, '', concat('https://example.com/', toString(number * 7919))) AS s
FROM numbers(20000)
SETTINGS output_format_parquet_data_page_size = 256, output_format_parquet_batch_size = 16,
    output_format_parquet_write_page_index = 1, output_format_parquet_row_group_size = 5000;

DROP TABLE IF EXISTS t_file;
DROP TABLE IF EXISTS t_merge;
CREATE TABLE t_file (n UInt32, s String) ENGINE = File(Parquet);
INSERT INTO t_file SELECT * FROM file(currentDatabase() || '_05331_plain.parquet');
CREATE TABLE t_merge AS t_file ENGINE = Merge(currentDatabase(), '^t_file$');

SELECT '-- plan: s.size is read and filtered in PREWHERE, s is not read';
SELECT countIf(explain ILIKE '%Prewhere filter column:%s.size != 0%'), countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT n % 7 AS k, avg(length(s)) FROM file(currentDatabase() || '_05331_plain.parquet') WHERE s != '' GROUP BY k);
SELECT countIf(explain ILIKE '%Prewhere filter column:%s.size != 0%'), countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT n FROM t_file WHERE notEmpty(s));
SELECT countIf(explain ILIKE '%Prewhere filter column:%s.size = 0%'), countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT n FROM file(currentDatabase() || '_05331_plain.parquet') WHERE empty(s));
SELECT countIf(match(explain, 'Output: (.+, )?s\\.size(, |$)')) > 0, countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT length(s) FROM file(currentDatabase() || '_05331_plain.parquet'));

SELECT '-- no rewrite: Nullable, LowCardinality, FixedString, tuple element, DEFAULT, CSV, Merge';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 's Nullable(String)')) WHERE explain ILIKE '%column_name: s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 's LowCardinality(String)')) WHERE explain ILIKE '%column_name: s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 's FixedString(4)')) WHERE explain ILIKE '%column_name: s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(t.s) FROM file('nonexistent_05331.parquet', Parquet, 't Tuple(s String)')) WHERE explain ILIKE '%column_name: t.s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 'n UInt32, s String DEFAULT \'abc\'')) WHERE explain ILIKE '%column_name: s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.csv', CSV, 's String')) WHERE explain ILIKE '%column_name: s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM t_merge) WHERE explain ILIKE '%column_name: s.size%';
SELECT '-- positive control';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 's String')) WHERE explain ILIKE '%column_name: s.size%';

SELECT '-- results: PLAIN, dictionary, small pages with a page index, File engine, Merge';
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_plain.parquet');
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_plain.parquet') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_dict.parquet');
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_dict.parquet') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(n), sum(length(s)) FROM file(currentDatabase() || '_05331_pages.parquet') WHERE s != '' AND n BETWEEN 3000 AND 3500;
SELECT count(), sum(n), sum(length(s)) FROM file(currentDatabase() || '_05331_pages.parquet') WHERE s != '' AND n BETWEEN 3000 AND 3500 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(n), sum(length(s)) FROM t_file WHERE notEmpty(s) AND n % 3 = 1;
SELECT count(), sum(n), sum(length(s)) FROM t_file WHERE notEmpty(s) AND n % 3 = 1 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(length(s)) FROM t_merge WHERE empty(s) OR length(s) > 25;
SELECT count(), sum(length(s)) FROM t_merge WHERE empty(s) OR length(s) > 25 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- results: block sizes';
SELECT sum(length(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 's String') SETTINGS max_block_size = 1, input_format_parquet_max_block_size = 1;
SELECT sum(length(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 's String') SETTINGS max_block_size = 7, input_format_parquet_max_block_size = 7;
SELECT sum(length(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_plain.parquet') SETTINGS max_block_size = 65505, input_format_parquet_max_block_size = 65505;
SELECT sum(length(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 's String') SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- results: DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY, nulls read as non-Nullable String, dictionary falling back to PLAIN';
SELECT sum(length(s)), sum(length(l)), sum(length(o)), sum(length(f)), countIf(empty(s)), countIf(o != ''), countIf(notEmpty(f))
FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String');
SELECT sum(length(s)), sum(length(l)), sum(length(o)), sum(length(f)), countIf(empty(s)), countIf(o != ''), countIf(notEmpty(f))
FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(n), sum(length(s)), sum(length(l)), sum(length(o)), sum(length(f))
FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String') WHERE n % 5 = 2;
SELECT count(), sum(n), sum(length(s)), sum(length(l)), sum(length(o)), sum(length(f))
FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String') WHERE n % 5 = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String') WHERE l != '' AND f != '' AND o != '';
SELECT count() FROM file(currentDatabase() || '_05331_fixture.parquet', Parquet, 'n Int32, s String, l String, o String, f String') WHERE l != '' AND f != '' AND o != '' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(length(value)), countIf(value != '') FROM file(currentDatabase() || '_05331_decimal.parquet', Parquet, 'keep Int32, value String');
SELECT sum(length(value)), countIf(value != '') FROM file(currentDatabase() || '_05331_decimal.parquet', Parquet, 'keep Int32, value String') SETTINGS optimize_functions_to_subcolumns = 0;

-- An integer column with a bloom filter and dictionary pages, read as String: the length constant must not
-- be looked up in the bloom filter or the dictionary as if it were one of the integers.
SELECT '-- results: no pruning by the sizes';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_int.parquet', Parquet)
SELECT toInt64(100 + number % 900) AS x FROM numbers(100000)
SETTINGS output_format_parquet_row_group_size = 10000, output_format_parquet_write_bloom_filter = 1;
SELECT count() FROM file(currentDatabase() || '_05331_int.parquet', Parquet, 'x String') WHERE length(x) = 3;
SELECT count() FROM file(currentDatabase() || '_05331_int.parquet', Parquet, 'x String') WHERE length(x) IN (3, 4);
SELECT count() FROM file(currentDatabase() || '_05331_int.parquet', Parquet, 'x String') WHERE length(x) = 3
SETTINGS input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
SELECT count() FROM file(currentDatabase() || '_05331_int.parquet', Parquet, 'x String') WHERE length(x) = 3 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- s and s.size together';
SELECT count(), sum(length(s)), max(s) FROM file(currentDatabase() || '_05331_plain.parquet') WHERE s.size > 27;
SELECT count(), sum(length(s)), max(s) FROM file(currentDatabase() || '_05331_plain.parquet') WHERE length(s) > 27;
SELECT count() FROM file(currentDatabase() || '_05331_plain.parquet') PREWHERE s.size != 0;
SELECT count() FROM file(currentDatabase() || '_05331_plain.parquet') PREWHERE notEmpty(s);
SELECT s, s.size FROM file(currentDatabase() || '_05331_dict.parquet') WHERE s.size = 3 ORDER BY s LIMIT 3;

-- `s.size` that is the sizes of `s` and a column named `s.size` are told apart.
SELECT '-- a column named s.size';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_named.parquet', Parquet) SELECT 'abc' AS s, 7::UInt64 AS `s.size` FROM numbers(3);
SELECT `s.size` FROM file(currentDatabase() || '_05331_named.parquet');
SELECT s, `s.size` FROM file(currentDatabase() || '_05331_named.parquet');
SELECT * FROM file(currentDatabase() || '_05331_named.parquet');
SELECT length(s) FROM file(currentDatabase() || '_05331_named.parquet');
SELECT sum(length(s)), sum(s.size), countIf(s != '') FROM file(currentDatabase() || '_05331_named.parquet', Parquet, 's String');
SELECT count() FROM file(currentDatabase() || '_05331_named.parquet', Parquet, 's String') PREWHERE s != '';
SELECT sum(n), sum(length(s)), sum(`s.size`) FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, s String, `s.size` UInt64');
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file('nonexistent_05331.parquet', Parquet, 's String, `S.SIZE` UInt64')) WHERE explain ILIKE '%column_name: s.size%';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_ci.parquet', Parquet) SELECT 'abc' AS s, 7::UInt64 AS `S.SIZE` FROM numbers(3);
SELECT sum(length(s)), sum(`S.SIZE`), sum(s.size) FROM file(currentDatabase() || '_05331_ci.parquet', Parquet, 's String, `S.SIZE` UInt64')
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
SELECT sum(length(s)), sum(s.size), countIf(notEmpty(s)) FROM file(currentDatabase() || '_05331_ci.parquet', Parquet, 's String')
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;

SELECT '-- a String column read from a tuple field';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_flat.parquet', Parquet)
SELECT toUInt32(number) AS n, if(number % 5 = 0, NULL, tuple(if(number % 3 = 0, '', toString(number * 7919))))::Nullable(Tuple(s String)) AS t
FROM numbers(20);
SELECT sum(length(`t.s`)), countIf(notEmpty(`t.s`)) FROM file(currentDatabase() || '_05331_flat.parquet', Parquet, 'n UInt32, `t.s` String');
SELECT sum(length(`t.s`)), countIf(notEmpty(`t.s`)) FROM file(currentDatabase() || '_05331_flat.parquet', Parquet, 'n UInt32, `t.s` String')
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(n) FROM file(currentDatabase() || '_05331_flat.parquet', Parquet, 'n UInt32, `t.s` String') WHERE `t.s` != '';
SELECT count(), sum(n) FROM file(currentDatabase() || '_05331_flat.parquet', Parquet, 'n UInt32, `t.s` String') WHERE `t.s` != ''
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- a DEFAULT column missing from the file';
SELECT sum(s.size), sum(length(s)), countIf(s != '') FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, x String DEFAULT \'abc\', s String');
SELECT sum(x.size), sum(length(x)), countIf(x != '') FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, x String DEFAULT \'abc\'');
SELECT '-- a DEFAULT column missing from the file, computed from s';
SELECT sum(length(s)), sum(length(d)) FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, s String, d String DEFAULT concat(s, \'!\')');
SELECT count(), sum(length(d)) FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, s String, d String DEFAULT concat(s, \'!\')') WHERE notEmpty(s);
SELECT sum(s.size), sum(length(d)) FROM file(currentDatabase() || '_05331_plain.parquet', Parquet, 'n UInt32, s String, d String DEFAULT concat(s, \'!\')')
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- a hive partition column';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_hive/s=abc/data.parquet', Parquet) SELECT toUInt32(number) AS n FROM numbers(10);
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM file(currentDatabase() || '_05331_hive/*/*.parquet', Parquet, 'n UInt32, s String') SETTINGS use_hive_partitioning = 1)
WHERE explain ILIKE '%column_name: s.size%';
SELECT sum(length(s)), countIf(s != ''), count() FROM file(currentDatabase() || '_05331_hive/*/*.parquet', Parquet, 'n UInt32, s String') SETTINGS use_hive_partitioning = 1;

SELECT '-- an AggregateFunction column read from its values';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_agg.parquet', Parquet)
SELECT if(number % 4 = 0, '', concat('v_', toString(number))) AS s, toUInt64(number) AS v FROM numbers(100);
SELECT sum(length(s)), countIf(s != ''), sum(finalizeAggregation(v))
FROM file(currentDatabase() || '_05331_agg.parquet', Parquet, 's String, v AggregateFunction(sum, UInt64)')
SETTINGS aggregate_function_input_format = 'value';
SELECT count(), sum(finalizeAggregation(v))
FROM file(currentDatabase() || '_05331_agg.parquet', Parquet, 's String, v AggregateFunction(sum, UInt64)') WHERE notEmpty(s)
SETTINGS aggregate_function_input_format = 'value';
SELECT sum(length(s)), countIf(s != ''), sum(finalizeAggregation(v))
FROM file(currentDatabase() || '_05331_agg.parquet', Parquet, 's String, v AggregateFunction(sum, UInt64)')
SETTINGS aggregate_function_input_format = 'value', optimize_functions_to_subcolumns = 0;

-- The table keeps the structure of its creation, `s Nullable(String)`, while the file now holds `s String`.
SELECT '-- a table function whose schema changed';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_drift.parquet', Parquet) SELECT toUInt32(number) AS n, toNullable(toString(number)) AS s FROM numbers(10);
CREATE TABLE t_drift AS file(currentDatabase() || '_05331_drift.parquet');
INSERT INTO FUNCTION file(currentDatabase() || '_05331_drift.parquet', Parquet) SELECT toUInt32(number) AS n, if(number < 5, '', toString(number * 1000)) AS s FROM numbers(10);
DETACH TABLE t_drift;
ATTACH TABLE t_drift;
SELECT count() FROM t_drift PREWHERE s.size > 3; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM t_drift WHERE length(s) > 3;
DROP TABLE t_drift;

SELECT '-- a table function whose schema changed, with a column named s.size';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_drift_named.parquet', Parquet) SELECT toNullable('abc') AS s, 7::UInt64 AS `s.size` FROM numbers(3);
CREATE TABLE t_drift_named AS file(currentDatabase() || '_05331_drift_named.parquet');
INSERT INTO FUNCTION file(currentDatabase() || '_05331_drift_named.parquet', Parquet) SELECT 'abcd' AS s, 7::UInt64 AS `s.size` FROM numbers(3);
DETACH TABLE t_drift_named;
ATTACH TABLE t_drift_named;
SELECT count(), sum(`s.size`) FROM t_drift_named PREWHERE `s.size` = 7;
DROP TABLE t_drift_named;
SELECT '-- a table function whose file gained a column named s.size';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_gained.parquet', Parquet) SELECT 'abcd' AS s FROM numbers(3);
CREATE TABLE t_gained AS file(currentDatabase() || '_05331_gained.parquet');
INSERT INTO FUNCTION file(currentDatabase() || '_05331_gained.parquet', Parquet) SELECT 'abcd' AS s, 7::UInt64 AS `s.size` FROM numbers(3);
DETACH TABLE t_gained;
ATTACH TABLE t_gained;
SELECT sum(length(s)), countIf(s != '') FROM t_gained;
SELECT sum(length(s)), countIf(s != '') FROM t_gained SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_gained;
SELECT '-- the same, and s changed type';
INSERT INTO FUNCTION file(currentDatabase() || '_05331_gained_drift.parquet', Parquet) SELECT 'abcd' AS s FROM numbers(3);
CREATE TABLE t_gained_drift AS file(currentDatabase() || '_05331_gained_drift.parquet');
INSERT INTO FUNCTION file(currentDatabase() || '_05331_gained_drift.parquet', Parquet) SELECT toNullable('abcd') AS s, 7::UInt64 AS `s.size` FROM numbers(3);
DETACH TABLE t_gained_drift;
ATTACH TABLE t_gained_drift;
SELECT sum(length(s)), countIf(s != '') FROM t_gained_drift;
SELECT sum(length(s)), countIf(s != '') FROM t_gained_drift SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_gained_drift;

SELECT '-- lazy materialization';
SET query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_file = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SELECT count() FROM (EXPLAIN SELECT n, length(s) FROM file(currentDatabase() || '_05331_plain.parquet') ORDER BY n DESC LIMIT 5) WHERE explain ILIKE '%LazilyReadFromFile%';
SELECT n, length(s) FROM file(currentDatabase() || '_05331_plain.parquet') ORDER BY n DESC LIMIT 5;
SELECT n, s.size FROM file(currentDatabase() || '_05331_plain.parquet') ORDER BY n DESC LIMIT 5;

DROP TABLE t_merge;
DROP TABLE t_file;
EOF

# S3 and URL go through the object storage and URL sources.
S3_PATH="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_05331/data.parquet"
${CLICKHOUSE_CLIENT} --query "
${SETTINGS}
INSERT INTO FUNCTION s3('${S3_PATH}', 'test', 'testtest', 'Parquet')
SELECT toUInt32(number) AS n, if(number % 4 = 0, '', concat('https://example.com/', toString(number * 7919))) AS s
FROM numbers(20000) SETTINGS s3_truncate_on_insert = 1;

SELECT '-- s3';
SELECT countIf(explain ILIKE '%Prewhere filter column:%s.size != 0%'), countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT n FROM s3('${S3_PATH}', 'test', 'testtest', 'Parquet') WHERE s != '');
SELECT count(), sum(n), sum(length(s)) FROM s3('${S3_PATH}', 'test', 'testtest', 'Parquet') WHERE s != '';
SELECT count(), sum(n), sum(length(s)) FROM s3('${S3_PATH}', 'test', 'testtest', 'Parquet') WHERE s != '' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT n, length(s) FROM s3('${S3_PATH}', 'test', 'testtest', 'Parquet') ORDER BY n DESC LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_object_storage = 1;

SELECT '-- url';
SELECT countIf(explain ILIKE '%Prewhere filter column:%s.size != 0%'), countIf(match(explain, 'Output: (.+, )?s(, |$)'))
FROM (EXPLAIN actions = 1 SELECT n FROM url('${S3_PATH}', 'Parquet') WHERE s != '');
SELECT count(), sum(n), sum(length(s)) FROM url('${S3_PATH}', 'Parquet') WHERE s != '';
"

# A data lake may hold files in other formats, so it reads the strings.
ICEBERG_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05331_iceberg/"
${CLICKHOUSE_CLIENT} --query "
${SETTINGS}
SET allow_insert_into_iceberg = 1;
DROP TABLE IF EXISTS t_iceberg;
CREATE TABLE t_iceberg (n UInt32, s String) ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
INSERT INTO t_iceberg SELECT toUInt32(number), if(number % 4 = 0, '', toString(number)) FROM numbers(1000);
SELECT '-- iceberg';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT length(s) FROM t_iceberg) WHERE explain ILIKE '%column_name: s.size%';
SELECT sum(length(s)), countIf(s != '') FROM t_iceberg;
DROP TABLE t_iceberg;
"

rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}_05331"*
