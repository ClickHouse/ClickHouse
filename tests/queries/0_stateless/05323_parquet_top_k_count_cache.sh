#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# An ORDER BY ... LIMIT read that skips row groups with the TopN filter must not cache the number of
# rows it read as the number of rows in the file, which `count()` then answers from.
# https://github.com/ClickHouse/ClickHouse/issues/123716

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05323_parquet_topk_count_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# One thread reads `part1` first, so its rows set the threshold and every row group of `part2` is skipped.
LOCAL=($CLICKHOUSE_LOCAL
    --use_top_k_dynamic_filtering=1
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --query_plan_optimize_lazy_materialization_for_file=0
    --optimize_count_from_files=1
    --use_cache_for_count_from_files=1
    --max_threads=1 --max_parsing_threads=1)

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/part1.parquet', Parquet) SELECT number AS k FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${DIR}/part2.parquet', Parquet) SELECT 10000 + number AS k FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;"

# A cached row count is used only for a file modified before the second it was cached in.
touch -d '1 hour ago' "${DIR}/part1.parquet" "${DIR}/part2.parquet"

# The TopN read and the counts run in one process, so they share the cache.
"${LOCAL[@]}" --query "
    SELECT k FROM file('${DIR}/part{1,2}.parquet', Parquet) ORDER BY k LIMIT 3;
    SELECT _file, count() FROM file('${DIR}/part{1,2}.parquet', Parquet) GROUP BY _file ORDER BY _file;
    SELECT _file, count() FROM file('${DIR}/part{1,2}.parquet', Parquet) GROUP BY _file ORDER BY _file
    SETTINGS use_cache_for_count_from_files = 0;"
