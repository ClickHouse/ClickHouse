#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (MinIO)

# Regression test for https://github.com/ClickHouse/ClickHouse/pull/107567.
#
# `s3_list_object_parallelism` must stay a *pure performance knob*: enabling it may only change how fast
# globbed S3 paths are listed, never the query result. The parallel listing iterator emits keys in
# scheduler (non-deterministic) order rather than S3's lexicographic order, and three call sites take the
# *first* listed file as meaningful:
#   * hive-partitioning detection          -> StorageObjectStorage::getPathSample
#   * cluster hive-partitioning detection  -> StorageObjectStorageCluster::getPathSample
#   * `format = 'auto'` / schema inference  -> StorageObjectStorage::createReadBufferIterator
# Both are therefore forced onto the serial (lexicographic) iterator. 04339 already proves that the *set
# of listed paths* is parallelism-independent; this test proves that the *inferred output* is too, which
# is what those call sites actually depend on. If a later refactor routes any site back through
# the parallel iterator, the DESCRIBE below stops being identical for parallelism 1 vs 8 and this fails.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04498"

# Compare the full inferred DESCRIBE (column names AND types) of a glob between serial and parallel
# listing. Both sides run against the same server, so an identical, non-empty result means the setting
# did not influence inference. A parallelism-dependent result would print a MISMATCH diagnostic instead.
compare() {
    local label="$1" table_function="$2" extra_settings="$3"
    local serial parallel
    serial=$($CLICKHOUSE_CLIENT -q "DESCRIBE ${table_function} SETTINGS ${extra_settings}s3_list_object_parallelism=1")
    parallel=$($CLICKHOUSE_CLIENT -q "DESCRIBE ${table_function} SETTINGS ${extra_settings}s3_list_object_parallelism=8")
    if [ -n "$serial" ] && [ "$serial" == "$parallel" ]; then
        echo "${label}: inference is parallelism-invariant"
    else
        echo "${label}: MISMATCH serial=[${serial}] parallel=[${parallel}]"
    fi
}

# --- Hive-partition detection (getPathSample). Each file sits under a *different* partition key, so the
# --- detected partition column is whichever path is sampled first; parallel listing would make it
# --- depend on scheduling. With the fix the sample is always the lexicographically-first key.
for k in a b c d e f g h; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/hive/${k}=1/data.csv', 'test', 'testtest', 'CSV', 'x UInt64') SELECT 1 SETTINGS s3_truncate_on_insert=1;"
done
compare "hive" "s3('${base}/hive/*/data.csv', 'test', 'testtest')" "use_hive_partitioning=1, "
compare "hive cluster" "s3Cluster('test_cluster_one_shard_three_replicas_localhost', '${base}/hive/*/data.csv', 'test', 'testtest')" "use_hive_partitioning=1, "

# --- Schema inference (createReadBufferIterator). Each subdirectory holds a CSV with a different number
# --- of columns, so the inferred schema is whichever file is read first; parallel listing would make it
# --- depend on scheduling. With the fix the first read file is always the lexicographically-first one.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/schema/d1/f.csv', 'test', 'testtest', 'CSV', 'a UInt64') SELECT 1 SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/schema/d2/f.csv', 'test', 'testtest', 'CSV', 'a UInt64, b UInt64') SELECT 1, 2 SETTINGS s3_truncate_on_insert=1;"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/schema/d3/f.csv', 'test', 'testtest', 'CSV', 'a UInt64, b UInt64, c UInt64') SELECT 1, 2, 3 SETTINGS s3_truncate_on_insert=1;"
compare "schema" "s3('${base}/schema/*/f.csv', 'test', 'testtest')" "schema_inference_mode='default', "
