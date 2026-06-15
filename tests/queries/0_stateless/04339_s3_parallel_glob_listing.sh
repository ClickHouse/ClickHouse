#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (MinIO)

# Tests that listing globbed s3() paths in parallel (s3_list_object_parallelism > 1, which walks the
# common-prefix tree concurrently) returns exactly the same files as the serial listing, for a
# Hive-style hierarchical layout and for several glob shapes, including the "**" fallback.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04339"

# Hive-style layout: year=*/month=*/data.csv, plus some noise to exercise glob filtering and pruning.
for y in 2021 2022; do
    for m in 01 02; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/year=${y}/month=${m}/data.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1;"
    done
done
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/year=2021/month=01/extra.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT number FROM numbers(5) SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/year=2021/month=01/_SUCCESS.txt', 'test', 'testtest', 'CSV', 'x UInt64') SELECT 1 SETTINGS s3_truncate_on_insert=1;"

# For a glob, check that serial (parallelism=1) and parallel (parallelism=8) listing produce identical
# results (both the aggregate and the exact set of file paths), and print a deterministic summary.
check() {
    local glob="$1"
    local q="FROM s3('${base}/${glob}', 'test', 'testtest', 'CSV', 'x UInt64')"

    local agg="SELECT 'rows=' || toString(count()) || ' files=' || toString(uniqExact(_path)) ${q}"
    local files="SELECT arraySort(groupArray(_path)) ${q}"

    local agg_serial agg_parallel files_serial files_parallel
    agg_serial=$($CLICKHOUSE_CLIENT -q "${agg} SETTINGS s3_list_object_parallelism=1")
    agg_parallel=$($CLICKHOUSE_CLIENT -q "${agg} SETTINGS s3_list_object_parallelism=8")
    files_serial=$($CLICKHOUSE_CLIENT -q "${files} SETTINGS s3_list_object_parallelism=1")
    files_parallel=$($CLICKHOUSE_CLIENT -q "${files} SETTINGS s3_list_object_parallelism=8")

    local match=0
    if [ "$agg_serial" == "$agg_parallel" ] && [ "$files_serial" == "$files_parallel" ]; then
        match=1
    fi
    echo "${glob} match=${match} ${agg_parallel}"
}

check "year=*/month=*/data.csv"
check "year=2021/month=*/data.csv"
check "year=*/month=01/*.csv"
check "year=*/month=*/*"
check "**/data.csv"
