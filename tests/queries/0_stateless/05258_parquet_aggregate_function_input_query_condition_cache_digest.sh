#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: the query condition cache is server-wide and size-bounded, so a
# concurrent test can evict our entry between the two reads and turn the expected
# pruned repeat into a full re-read (same reason as `04658_parquet_file_engine_query_condition_cache_without_metadata_cache`)

# `aggregate_function_input_format` wraps the Parquet reader into `AggregateFunctionStatesFromValuesInputFormat`.
# The wrapper must forward `getFileMetadataDigest` together with `getMatchedBuckets`: otherwise the
# query condition cache entry is written with a zero footer digest, and the next identical read refuses
# to turn the hit into a row-group restriction, re-reading the whole file.
# The plain Parquet read is the control: both repeats must read only the matching row groups.
# The predicate is not prunable by the row-group min/max statistics, so only the cache can prune.
# Like `04658_parquet_file_engine_query_condition_cache_without_metadata_cache`, this needs a
# `File`-engine table in an `Atomic` database: table functions carry a nil storage UUID
# for which `QueryConditionCache::{read,write}` are no-ops.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query condition cache is fed by the filter the Parquet reader applies, so pin the `PREWHERE`
# optimization: with a randomized `query_plan_optimize_prewhere = 0` the reader may read every
# row group unfiltered, nothing is cached, and both repeats would read the whole file.
SETTINGS="use_query_condition_cache = 1, max_threads = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1"

TAG="05258_${CLICKHOUSE_DATABASE}"
DATA_FILE_RELATIVE="${CLICKHOUSE_TEST_UNIQUE_NAME}/05258.parquet"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DATA_FILE_RELATIVE}', Parquet, 'k UInt64, x UInt64') SELECT number, number * 10 FROM numbers(3200) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05258_plain"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05258_state"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05258_plain (k UInt64, x UInt64) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05258_state (k UInt64, x AggregateFunction(sum, UInt64)) ENGINE = File(Parquet, '${DATA_FILE_RELATIVE}') SETTINGS aggregate_function_input_format = 'value'"

# The cache is bypassed until the file's version token has settled
# (`file_version_settle_seconds = 3` in `StorageFile.cpp`), so give the file time to settle.
sleep 4

for run in first second
do
    ${CLICKHOUSE_CLIENT} --query "SELECT k, x FROM t_05258_plain WHERE k % 1000 = 175 ORDER BY k SETTINGS ${SETTINGS}, log_comment = '${TAG}_plain_${run}'"
done

for run in first second
do
    ${CLICKHOUSE_CLIENT} --query "SELECT k, sumMerge(x) FROM t_05258_state WHERE k % 1000 = 175 GROUP BY k ORDER BY k SETTINGS ${SETTINGS}, log_comment = '${TAG}_state_${run}'"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# 64 row groups of 50 rows, the predicate matches rows in 4 of them: a repeat that applies
# the cached marks reads only those 4 row groups.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(log_comment, '${TAG}_', ''),
        ProfileEvents['QueryConditionCacheHits'] AS hits,
        ProfileEvents['ParquetReadRowGroups'] AS read_row_groups
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment LIKE '${TAG}_%'
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05258_plain"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05258_state"
rm -r "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
