#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# `type_json_use_partial_match_to_skip_paths_by_regexp` changes the values of a `JSON` column read
# from a `Parquet` file without changing its type: a column cached with one mode must not be served
# to a read with the other one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

cp "$CUR_DIR/data_parquet/05299_variant_read_as_json.parquet" "${FILE_PATH}"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT n, v FROM file('${FILE}', Parquet, 'n Int64, v JSON(SKIP REGEXP \'c\')') ORDER BY n"
CACHE="use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
    columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The admission control of the cache can skip a write: read the file until the column of `v` is there.
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "${QUERY} FORMAT Null SETTINGS ${CACHE}, type_json_use_partial_match_to_skip_paths_by_regexp = 1"
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') AND column = 'v'")" = 1 ] && break
done

echo "--- partial match, from the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${CACHE}, type_json_use_partial_match_to_skip_paths_by_regexp = 1"
echo "--- full match, with the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${CACHE}, type_json_use_partial_match_to_skip_paths_by_regexp = 0"
echo "--- full match, without the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS use_columns_cache = 0, type_json_use_partial_match_to_skip_paths_by_regexp = 0"

rm -f "${FILE_PATH}"
