#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# An `ORDER BY ... LIMIT n` read of a Parquet file with TopN dynamic filtering (`__topKFilter`) returns
# only the rows that survive the running threshold. Their number must not be stored in the cache for
# `count()` from files as the total of the whole file, otherwise a later cached `count()` undercounts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_DATABASE}_05355.parquet"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${FILE}')
    SELECT number AS k FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"

# Backdate the file so its modification time has settled and the count cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH:?}/${FILE}"

SETTINGS="use_cache_for_count_from_files = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "SELECT k FROM file('${FILE}') ORDER BY k DESC LIMIT 1 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${FILE}') SETTINGS ${SETTINGS}"

rm -f "${USER_FILES_PATH:?}/${FILE}"
