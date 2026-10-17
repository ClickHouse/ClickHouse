#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', Parquet)
    SELECT number AS id, tuple(number, toString(number % 31)) AS tup FROM numbers(100000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 12000"

# The absolute row group count depends on write settings the test runner randomizes, so the
# assertion is that a tuple element predicate prunes exactly as much as the same predicate on
# a top-level column holding identical values, and that both prune something.
run() {
    local label="$1"
    local predicate="$2"
    local query_id="${CLICKHOUSE_DATABASE}_${label}_$RANDOM"

    ${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "
        SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE ${predicate}
        SETTINGS max_threads = 1, enable_analyzer = 1, optimize_functions_to_subcolumns = 1,
                 input_format_parquet_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 0 FORMAT Null"

    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        CREATE OR REPLACE VIEW pushdown_${label} AS
        SELECT ProfileEvents['ParquetReadRowGroups'] AS read, ProfileEvents['ParquetPrunedRowGroups'] AS pruned
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '${query_id}' AND type = 'QueryFinish'
          AND current_database = currentDatabase()"
}

run top_level "id = 55555"
run tuple_element "tup.1 = 55555"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'top level prunes', read = 1 AND pruned > 0 FROM pushdown_top_level;
    SELECT 'tuple element prunes identically', t.read = b.read AND t.pruned = b.pruned
    FROM pushdown_tuple_element AS t, pushdown_top_level AS b;
    SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE id = 55555;
    SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE tup.1 = 55555;
    SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE tup.2 = '7';"

# `a.b` and the nested `a`.`b` flatten to the same name, and the Parquet reader rejects the
# ambiguous name outright, so rewriting to it turns a valid query into DUPLICATE_COLUMN.
echo '-- a dotted element name colliding with a nested path returns the dotted element'
COLLISION_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_collision.parquet"
COLLISION_STRUCTURE='t Tuple(a Tuple(b UInt64), `a.b` UInt64)'

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${COLLISION_FILE}', Parquet, '${COLLISION_STRUCTURE}')
    SELECT tuple(tuple(111::UInt64), 999::UInt64) FROM numbers(2)
    SETTINGS engine_file_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT tupleElement(t, 'a.b'), tupleElement(tupleElement(t, 'a'), 'b')
    FROM file('${COLLISION_FILE}', Parquet, '${COLLISION_STRUCTURE}') LIMIT 1
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, input_format_parquet_filter_push_down = 1;
    SELECT countIf(tupleElement(t, 'a.b') = 999), countIf(tupleElement(t, 'a.b') = 111)
    FROM file('${COLLISION_FILE}', Parquet, '${COLLISION_STRUCTURE}')
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, input_format_parquet_filter_push_down = 1"

# An unnamed tuple names its elements `1`, `2`, which the reader can only match by string against
# the file's real field names, so the element must be read out of the whole tuple.
echo '-- a positional tuple hint reads element values, not defaults'
POSITIONAL_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_positional.parquet"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${POSITIONAL_FILE}', Parquet, 't Tuple(x UInt64, y String)')
    SELECT tuple(number, toString(number)) FROM numbers(3)
    SETTINGS engine_file_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT tupleElement(t, 1), tupleElement(t, 2)
    FROM file('${POSITIONAL_FILE}', Parquet, 't Tuple(UInt64, String)') ORDER BY 1
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;
    SELECT countIf(tupleElement(t, 1) = 1)
    FROM file('${POSITIONAL_FILE}', Parquet, 't Tuple(UInt64, String)')
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1"

# The same positional hint through a subquery: the subcolumn pushdown pass must keep reading
# the whole tuple as well instead of pushing the ordinal element name into the file read.
echo '-- a positional tuple hint through a subquery reads element values, not defaults'
${CLICKHOUSE_CLIENT} --query "
    SELECT tupleElement(t, 1), tupleElement(t, 2)
    FROM (SELECT t FROM file('${POSITIONAL_FILE}', Parquet, 't Tuple(UInt64, String)')) ORDER BY 1
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, optimize_push_subcolumns_into_subqueries = 1;
    SELECT countIf(tupleElement(t, 1) = 1)
    FROM (SELECT t FROM file('${POSITIONAL_FILE}', Parquet, 't Tuple(UInt64, String)'))
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, optimize_push_subcolumns_into_subqueries = 1"
