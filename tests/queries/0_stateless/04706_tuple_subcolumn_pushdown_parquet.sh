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
        SETTINGS max_threads = 1, optimize_functions_to_subcolumns = 1, input_format_parquet_filter_push_down = 1 FORMAT Null"

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
