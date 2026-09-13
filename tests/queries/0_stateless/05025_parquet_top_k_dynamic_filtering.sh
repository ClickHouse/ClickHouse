#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05025_parquet_topk_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# Pin everything the flaky check randomizes and everything the assertions depend on: the top-k
# settings, the native Parquet reader (the only one that consumes the top-k filter), its
# stats-based pruning, and single-threaded reading (the two-file pruning assertion below relies
# on the files being read one after another).
LOCAL=(${CLICKHOUSE_LOCAL}
    --use_top_k_dynamic_filtering_for_variable_length_types=0
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --max_block_size=65409
    --max_threads=1 --max_parsing_threads=1)
ON=(--use_top_k_dynamic_filtering=1)
OFF=(--use_top_k_dynamic_filtering=0)

# part1 is a single row group, so it can never be pruned as a whole: the `rows_read` bound below
# stays valid no matter how reading races with the sorting. part2 has 10 row groups, k strictly
# greater than everything in part1, and no nulls: once part1 has been read (files of a glob are
# read one after another in a single stream), the top-K threshold proves that no row group of
# part2 can contain a smaller k, and all of them are skipped without reading any column data.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/part1.parquet', Parquet)
    SELECT number AS k, number * 10 AS v, if(number % 3 = 0, NULL, toInt64(number % 1000)) AS n
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/part2.parquet', Parquet)
    SELECT 100000 + number AS k, number * 10 AS v, toInt64(number % 1000) AS n
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 10000, engine_file_truncate_on_insert = 1;
"

run_json() {
    "${LOCAL[@]}" "$@" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'all of part2 skipped:', d['statistics']['rows_read'] <= 100000)"
}

echo "-- row-group pruning: part1 establishes the threshold before part2 starts, so every row"
echo "-- group of part2 is skipped and at most the 100000 rows of part1 are read"
run_json "${ON[@]}" --query "SELECT k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 3"

echo "-- without the optimization all 200000 rows are read"
run_json "${OFF[@]}" --query "SELECT k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 3"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k DESC LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) WHERE v % 40 = 0 ORDER BY k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 7 OFFSET 13"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n NULLS FIRST, k LIMIT 7"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n NULLS LAST, k LIMIT 7"
    "SELECT k, n FROM file('${DIR}/part1.parquet', Parquet) ORDER BY n DESC NULLS FIRST, k LIMIT 7"
    "SELECT k, v FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k, v LIMIT 7"
    "SELECT k FROM file('${DIR}/part1.parquet', Parquet) WHERE k < 3 ORDER BY k LIMIT 100"
)
for query in "${queries[@]}"; do
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done

echo "-- sort keys the Parquet reader never sees: virtual columns and Hive partition columns are"
echo "-- appended after the file is read, so the reader-side filter must not be armed for them;"
echo "-- results with and without the optimization must agree, and no row group may be skipped"
mkdir -p "${DIR}/hive/p=2" "${DIR}/hive/p=1"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/hive/p=2/data.parquet', Parquet)
    SELECT number AS k FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 100, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/hive/p=1/data.parquet', Parquet)
    SELECT 1000 + number AS k FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 100, engine_file_truncate_on_insert = 1;
"
virtual_queries=(
    "SELECT _file, k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY _file DESC, k LIMIT 3"
    "SELECT _path LIKE '%part2%', k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY _path DESC, k LIMIT 3"
    "SELECT p, k FROM file('${DIR}/hive/**/*.parquet', Parquet) ORDER BY p, k LIMIT 3 SETTINGS use_hive_partitioning = 1"
    "SELECT p, k FROM file('${DIR}/hive/**/*.parquet', Parquet) ORDER BY p DESC, k DESC LIMIT 3 SETTINGS use_hive_partitioning = 1"
)
for query in "${virtual_queries[@]}"; do
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done
"${LOCAL[@]}" "${ON[@]}" --query "SELECT p, k FROM file('${DIR}/hive/**/*.parquet', Parquet) ORDER BY p, k LIMIT 3 SETTINGS use_hive_partitioning = 1"
echo "-- all rows of both Hive files are read (nothing is pruned by a filter the reader cannot evaluate)"
"${LOCAL[@]}" "${ON[@]}" --query "SELECT p, k FROM file('${DIR}/hive/**/*.parquet', Parquet) ORDER BY p, k LIMIT 3 SETTINGS use_hive_partitioning = 1" --format JSON | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('rows_read:', d['statistics']['rows_read'])"

echo "-- collated ORDER BY: Parquet min/max statistics are bytewise, not collation-ordered, so the"
echo "-- row-group shortcut must stay off; 'ä' sorts before 'b' in the 'de' locale but its UTF-8"
echo "-- bytes are above 'z', so a stats-based skip of the second file would lose it"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/coll1.parquet', Parquet)
    SELECT arrayJoin(['b', 'c', 'd']) AS s SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/coll2.parquet', Parquet)
    SELECT arrayJoin(['z', 'ä']) AS s SETTINGS engine_file_truncate_on_insert = 1;
"
collate_query="SELECT s FROM file('${DIR}/coll{1,2}.parquet', Parquet) ORDER BY s COLLATE 'de' LIMIT 3"
# `LOCAL` pins the variable-length opt-in to 0; a String sort column needs it on.
LOCAL_STRING=("${LOCAL[@]/--use_top_k_dynamic_filtering_for_variable_length_types=0/--use_top_k_dynamic_filtering_for_variable_length_types=1}")
"${LOCAL_STRING[@]}" "${ON[@]}" --query "${collate_query}"
diff \
    <("${LOCAL_STRING[@]}" "${ON[@]}" --query "${collate_query}") \
    <("${LOCAL_STRING[@]}" "${OFF[@]}" --query "${collate_query}") \
    && echo "OK"

echo "-- a sort key the reader does not physically read: a column with a DEFAULT expression that"
echo "-- the file does not store is filled with type defaults inside the reader and only computed"
echo "-- above it (AddingDefaultsTransform), so comparing those placeholders against a threshold"
echo "-- derived from the computed values would drop rows of the top-K"
# `d` is not stored in part1.parquet, so the reader fills it with zeros and only
# `AddingDefaultsTransform` above the reader computes the real values. Its top-K rows are the
# *last* rows of the file: if the reader applied the threshold to the zeros it puts in `d`'s
# place, everything after the first block would be dropped and the answer would come from the
# first block alone. (`k` has to be selected as well - the default expression reads it.)
default_structure="k UInt64, d UInt64 DEFAULT k"
default_queries=(
    "SELECT k, d FROM file('${DIR}/part1.parquet', Parquet, '${default_structure}') ORDER BY d DESC LIMIT 3"
    "SELECT k, d FROM file('${DIR}/part1.parquet', Parquet, '${default_structure}') ORDER BY d LIMIT 3"
    "SELECT k, d FROM file('${DIR}/part1.parquet', Parquet, 'k UInt64, d UInt64 DEFAULT 100000 - k') ORDER BY d LIMIT 3"
)
for query in "${default_queries[@]}"; do
    "${LOCAL[@]}" "${ON[@]}" --query "${query}"
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done

echo "-- floating-point sort keys: ORDER BY sorts 'nan' together with the NULLs while the reader's"
echo "-- comparison does not, and Parquet min/max statistics legally omit 'nan', so neither the"
echo "-- per-row filter nor the row-group shortcut may be armed for them (see issue #116705)"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/f1.parquet', Parquet)
    SELECT toFloat64(100 + number) AS f FROM numbers(65536)
    SETTINGS output_format_parquet_row_group_size = 65536, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/f2.parquet', Parquet)
    SELECT arrayJoin([toFloat64(1000), nan]) AS f
    SETTINGS engine_file_truncate_on_insert = 1;
"
# f1 is read first in the `f{1,2}` order and second in the `f{2,1}` order, which exercises both
# failure modes: a threshold established from finite values skipping the row group that holds the
# `nan` (its statistics do not mention it), and a `nan` becoming the threshold itself and then
# rejecting every finite value read afterwards.
float_queries=(
    "SELECT f FROM file('${DIR}/f{1,2}.parquet', Parquet) ORDER BY f ASC NULLS FIRST LIMIT 2"
    "SELECT f FROM file('${DIR}/f{1,2}.parquet', Parquet, 'f Nullable(Float64)') ORDER BY f ASC NULLS FIRST LIMIT 2"
    "SELECT f FROM file('${DIR}/f{1,2}.parquet', Parquet) ORDER BY f DESC NULLS FIRST LIMIT 2"
    "SELECT f FROM file('${DIR}/f{2,1}.parquet', Parquet) ORDER BY f LIMIT 2"
    "SELECT f FROM file('${DIR}/f{2,1}.parquet', Parquet) ORDER BY f DESC LIMIT 2"
)
for query in "${float_queries[@]}"; do
    "${LOCAL[@]}" "${ON[@]}" --query "${query}"
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done

echo "-- a statistic that cannot be decoded (here a negative Int64 read as UInt64) leaves the bound"
echo "-- at the Range infinity sentinel, which is a Null Field and would be compared as a SQL NULL;"
echo "-- an unbounded side can never prove exclusion, so the row group must not be skipped"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/stat1.parquet', Parquet)
    SELECT toInt64(1000 + number) AS k FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/stat2.parquet', Parquet)
    SELECT arrayJoin([toInt64(5), toInt64(6), toInt64(-1)]) AS k
    SETTINGS engine_file_truncate_on_insert = 1;
"
stat_query="SELECT k FROM file('${DIR}/stat{1,2}.parquet', Parquet, 'k UInt64') ORDER BY k LIMIT 3"
"${LOCAL[@]}" "${ON[@]}" --query "${stat_query}"
diff \
    <("${LOCAL[@]}" "${ON[@]}" --query "${stat_query}") \
    <("${LOCAL[@]}" "${OFF[@]}" --query "${stat_query}") \
    && echo "OK"

echo "-- a tuple-element sort key whose storage parent carries a DEFAULT. The reading step answers"
echo "-- from the very ColumnsDescription that AddingDefaultsTransform is built from, so a name it"
echo "-- reports as free of a default expression is by construction one the transform cannot"
echo "-- rewrite: here the subcolumn description carries no default, and correspondingly the"
echo "-- parent's DEFAULT is never applied (the null t.a rows read back as 0, not as k)"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/tup.parquet', Parquet)
    SELECT number AS k, tuple(if(number % 2 = 0, NULL, number), number)::Tuple(a Nullable(UInt64), b UInt64) AS t
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1;
"
tuple_structure="k UInt64, t Tuple(a UInt64, b UInt64) DEFAULT (k, k)"
tuple_queries=(
    "SELECT k, t.a FROM file('${DIR}/tup.parquet', Parquet, '${tuple_structure}') ORDER BY t.a DESC, k LIMIT 3 SETTINGS input_format_null_as_default = 1"
    "SELECT k, t.a FROM file('${DIR}/tup.parquet', Parquet, '${tuple_structure}') ORDER BY t.a, k LIMIT 3 SETTINGS input_format_null_as_default = 1"
    "SELECT k, t.a, t.b FROM file('${DIR}/tup.parquet', Parquet, '${tuple_structure}') ORDER BY t.a, k LIMIT 3 SETTINGS input_format_null_as_default = 1"
    "SELECT k, t, t.a FROM file('${DIR}/tup.parquet', Parquet, '${tuple_structure}') ORDER BY t.a DESC, k LIMIT 3 SETTINGS input_format_null_as_default = 1"
)
for query in "${tuple_queries[@]}"; do
    "${LOCAL[@]}" "${ON[@]}" --query "${query}"
    diff \
        <("${LOCAL[@]}" "${ON[@]}" --query "${query}") \
        <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") \
        && echo "OK"
done
