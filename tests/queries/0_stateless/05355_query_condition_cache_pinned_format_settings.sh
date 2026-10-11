#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# Two `File` tables without a UUID (in an `Ordinary` database) over the same Parquet file, with the same
# format, compression and structure, but with different reader settings fixed at `CREATE`. The query
# condition cache entries of one table must not be reused by the other: a table that fills the missing
# column `c` with defaults records that no row group matches, and a table that must throw for the missing
# column must not skip the file because of these entries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05355"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
DB="${CLICKHOUSE_DATABASE}_ordinary"

SETTINGS="use_query_condition_cache = 1, enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

# All values are even, so the odd value 3 never matches.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet') SELECT number * 2 AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"

# `--send_logs_level=fatal` keeps the `Ordinary` engine deprecation warning (emitted once per server
# lifetime, on the first CREATE of an `Ordinary` database) out of the test's stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_deprecated_database_ordinary 1 --query "CREATE DATABASE ${DB} ENGINE = Ordinary"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.t_allow (b UInt64, c UInt64) ENGINE = File(Parquet, '${DATA_DIR}/data.parquet')
    SETTINGS input_format_parquet_allow_missing_columns = 1"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.t_disallow (b UInt64, c UInt64) ENGINE = File(Parquet, '${DATA_DIR}/data.parquet')
    SETTINGS input_format_parquet_allow_missing_columns = 0"

${CLICKHOUSE_CLIENT} --query "SELECT 'allow', sum(c) FROM ${DB}.t_allow WHERE b = 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT 'allow', sum(c) FROM ${DB}.t_allow WHERE b = 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT 'disallow', sum(c) FROM ${DB}.t_disallow WHERE b = 3 SETTINGS ${SETTINGS}" \
    >/dev/null 2>&1 || echo "disallow: exception"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
