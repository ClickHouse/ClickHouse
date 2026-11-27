#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# `min` / `max` answered from Parquet statistics read rows built from the statistics instead of the rows of the
# file. Their number must not be cached as the number of rows in the file, which `count()` then answers from.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05332_parquet_min_max_count_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

LOCAL=($CLICKHOUSE_LOCAL
    --optimize_min_max_from_files=1
    --optimize_count_from_files=1
    --use_cache_for_count_from_files=1)

# Several row groups with some NULLs, only NULLs, and no rows.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/a.parquet', Parquet) SELECT if(number % 7 = 0, NULL, number) AS x FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${DIR}/b.parquet', Parquet) SELECT CAST(NULL AS Nullable(UInt64)) AS x FROM numbers(3000)
    SETTINGS engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${DIR}/c.parquet', Parquet) SELECT toNullable(number) AS x FROM numbers(0)
    SETTINGS engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${DIR}/s.parquet', Parquet) SELECT toString(number) AS s FROM numbers(5000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;"

# A cached row count is used only for a file modified before the second it was cached in.
touch -d '1 hour ago' "${DIR}/a.parquet" "${DIR}/b.parquet" "${DIR}/c.parquet"

# The min/max reads and the counts run in one process, so they share the cache.
"${LOCAL[@]}" --query "
    SELECT min(x), max(x) FROM file('${DIR}/a.parquet', Parquet);
    SELECT value FROM system.events WHERE event = 'ParquetReadMinMaxFromStatistics';
    SELECT count() FROM file('${DIR}/a.parquet', Parquet);
    SELECT count() FROM file('${DIR}/a.parquet', Parquet) SETTINGS use_cache_for_count_from_files = 0;
    SELECT min(x), max(x) FROM file('${DIR}/{a,b,c}.parquet', Parquet);
    SELECT value FROM system.events WHERE event = 'ParquetReadMinMaxFromStatistics';
    SELECT _file, count() FROM file('${DIR}/{a,b,c}.parquet', Parquet) GROUP BY _file ORDER BY _file;
    SELECT _file, count() FROM file('${DIR}/{a,b,c}.parquet', Parquet) GROUP BY _file ORDER BY _file
    SETTINGS use_cache_for_count_from_files = 0;
    SELECT count() FROM file('${DIR}/c.parquet', Parquet);"

# `String` statistics are not used, so this reads every row and caches their number. The structure is given, so schema
# inference does not cache the number of rows from the metadata first.
"${LOCAL[@]}" --query "
    SELECT min(s), max(s) FROM file('${DIR}/s.parquet', Parquet, 's String');
    SELECT number_of_rows FROM system.schema_inference_cache WHERE source LIKE '%/s.parquet';"
