#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# ^ no-fasttest: needs Parquet support.
# no-parallel-replicas: aggregate bytes accounting differs with parallel replicas.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105333
#
# `Parquet::ReadManager` used to report `read_bytes` as the entire row group's
# `total_compressed_size`, summing every column in the file rather than only the
# columns selected by the query. For a 10-column file selecting one column,
# that overcounted by 10x. This test writes such a file and verifies that
# selecting one column reports far fewer bytes than selecting all of them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_DATABASE}_04260.parquet"
TAG_ONE="${CLICKHOUSE_DATABASE}_04260_one_col"
TAG_ALL="${CLICKHOUSE_DATABASE}_04260_all_cols"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', 'Parquet')
    SELECT
        repeat(cityHash64(number)::String,   4) AS c0,
        repeat(cityHash64(number+1)::String, 4) AS c1,
        repeat(cityHash64(number+2)::String, 4) AS c2,
        repeat(cityHash64(number+3)::String, 4) AS c3,
        repeat(cityHash64(number+4)::String, 4) AS c4,
        repeat(cityHash64(number+5)::String, 4) AS c5,
        repeat(cityHash64(number+6)::String, 4) AS c6,
        repeat(cityHash64(number+7)::String, 4) AS c7,
        repeat(cityHash64(number+8)::String, 4) AS c8,
        repeat(cityHash64(number+9)::String, 4) AS c9
    FROM numbers(50000)
    SETTINGS engine_file_truncate_on_insert = 1, max_insert_threads = 1
"

# Read just one column.
$CLICKHOUSE_CLIENT --log_comment "$TAG_ONE" --query "
    SELECT sum(ignore(c0)) FROM file('${PARQUET_FILE}', 'Parquet') SETTINGS enable_filesystem_cache = 0
"

# Read all ten columns.
$CLICKHOUSE_CLIENT --log_comment "$TAG_ALL" --query "
    SELECT sum(ignore(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)) FROM file('${PARQUET_FILE}', 'Parquet') SETTINGS enable_filesystem_cache = 0
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# With the fix, read_bytes for one column should be roughly one tenth of the
# read for all ten columns. We require it to be at most one fifth, leaving
# plenty of slack for footer/page-header overhead. Before the fix both queries
# reported the same row-group total_compressed_size, so this assertion failed.
$CLICKHOUSE_CLIENT --query "
    SELECT
        if(read_bytes_one_col * 5 < read_bytes_all_cols, 'ok',
           format('FAIL: one_col={} all_cols={}', read_bytes_one_col, read_bytes_all_cols)) AS read_bytes_only_selected_columns
    FROM
    (
        SELECT
            sumIf(read_bytes, log_comment = '${TAG_ONE}') AS read_bytes_one_col,
            sumIf(read_bytes, log_comment = '${TAG_ALL}') AS read_bytes_all_cols
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND (log_comment = '${TAG_ONE}' OR log_comment = '${TAG_ALL}')
          AND type = 'QueryFinish'
    )
"
