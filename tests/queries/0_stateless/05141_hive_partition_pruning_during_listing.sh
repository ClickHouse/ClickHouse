#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: requires S3 storage
# Tag no-parallel-replicas: relies on query_log which does not account other replicas

# Hive-style partitioned directories are pruned while listing: every `name=value` level covered by the glob is
# enumerated with delimiter listing and the directories that cannot satisfy the condition are never listed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url_prefix="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05141_hive"
structure="id UInt64, year UInt16, country String"

idx=0
for year in 2024 2025 2026; do
    idx=0
    for country in DE FR US; do
        idx=$((idx + 1))
        $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${url_prefix}/year=${year}/country=${country}/data.parquet', 'test', 'testtest', 'Parquet', 'id UInt64') SELECT toUInt64(${year} * 10 + ${idx}) SETTINGS s3_truncate_on_insert = 1"
    done
done

# $1 - case name, $2 - query, $3 - extra settings, $4 - whether to print the directory listing counters
function run_case()
{
    local query_id="05141_${1}_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
    echo "-- $1"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "$2 SETTINGS use_hive_partitioning = 1, log_queries = 1${3}"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    if [ "$4" = "1" ]; then
        columns="ProfileEvents['ObjectStorageListedObjects'] AS listed_objects, ProfileEvents['ObjectStorageReadObjects'] AS read_objects, ProfileEvents['ObjectStorageListedCommonPrefixes'] AS listed_directories, ProfileEvents['ObjectStorageHivePartitionPrunedPrefixes'] AS pruned_directories"
    else
        columns="ProfileEvents['ObjectStorageListedObjects'] AS listed_objects, ProfileEvents['ObjectStorageReadObjects'] AS read_objects"
    fi
    $CLICKHOUSE_CLIENT -q "
SELECT ${columns}
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish' AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1"
}

globbed="s3('${url_prefix}/year=*/country=*/*.parquet', 'test', 'testtest', 'Parquet', '${structure}')"

run_case both_levels "SELECT id FROM ${globbed} WHERE year = 2025 AND country = 'FR' ORDER BY id" "" 1
run_case disabled "SELECT id FROM ${globbed} WHERE year = 2025 AND country = 'FR' ORDER BY id" ", use_hive_partition_pruning_during_listing = 0" 1
run_case range_and_in "SELECT id FROM ${globbed} WHERE year >= 2025 AND country IN ('DE', 'US') ORDER BY id" "" 1
run_case second_level_only "SELECT id FROM ${globbed} WHERE country = 'US' ORDER BY id" "" 1
run_case no_matching_partition "SELECT count() FROM ${globbed} WHERE year = 1999" "" 1
run_case condition_on_file_column_and_virtual_column "SELECT id FROM ${globbed} WHERE year = 2025 AND id > 20251 AND _file = 'data.parquet' ORDER BY id" "" 1
run_case or_condition "SELECT id FROM ${globbed} WHERE (year = 2024 AND country = 'DE') OR (year = 2026 AND country = 'US') ORDER BY id" "" 0
run_case max_prefixes_fallback "SELECT id FROM ${globbed} WHERE country = 'US' ORDER BY id" ", hive_partition_pruning_during_listing_max_prefixes = 2" 1
run_case literal_first_level "SELECT id FROM s3('${url_prefix}/year=2024/country=*/*.parquet', 'test', 'testtest', 'Parquet', '${structure}') WHERE country = 'FR' ORDER BY id" "" 1
run_case partial_literal_in_glob "SELECT id FROM s3('${url_prefix}/year=202*/country=*/*.parquet', 'test', 'testtest', 'Parquet', '${structure}') WHERE year = 2026 AND country = 'DE' ORDER BY id" "" 1
run_case double_star_only "SELECT id FROM s3('${url_prefix}/**.parquet', 'test', 'testtest', 'Parquet', '${structure}') WHERE year = 2025 AND country = 'FR' ORDER BY id" "" 1
run_case level_before_double_star "SELECT id FROM s3('${url_prefix}/year=*/**.parquet', 'test', 'testtest', 'Parquet', '${structure}') WHERE year = 2026 ORDER BY id" "" 1
