#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_concurrency_scaling"
bad_table="${CLICKHOUSE_DATABASE}.overwrite_cache_bad_admission"
writer_pids=()
reader_pids=()
reader_dir=$(mktemp -d "$CLICKHOUSE_TMP/overwrite-cache-parallel-readers-XXXXXX")

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_commit" >/dev/null 2>&1 ||:
    for pid in "${writer_pids[@]}" "${reader_pids[@]}"
    do
        if [[ -n "$pid" ]]
        then
            wait "$pid" >/dev/null 2>&1 ||:
        fi
    done
    rm -rf "$reader_dir"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $bad_table" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $table
    (
        key UInt64,
        bucket UInt8,
        version UInt64,
        payload UInt64
    )
    ENGINE = OverwriteCache(version)
    KEYS (key, bucket)
    INDEX (bucket)
    SETTINGS
        max_memory_bytes = 1073741824,
        max_pending_insert_bytes = 134217728,
        max_concurrent_insert_preparations = 8,
        max_insert_publication_threads = 4"

$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 1, 0 FROM numbers(40000)"

if $CLICKHOUSE_CLIENT -q "
        EXPLAIN PIPELINE compact = 0
        SELECT sum(payload)
        FROM $table
        WHERE bucket = 1
        SETTINGS max_threads = 4, max_block_size = 8192" | grep -q 'FilterTransform.*4'
then
    echo "parallel read pipeline"
else
    echo "single read pipeline"
fi

for writer in $(seq 1 10)
do
    version=$((writer + 1))
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $table
        SELECT number, 1, $version, $writer
        FROM numbers(5000)" >/dev/null &
    writer_pids+=("$!")
done
for pid in "${writer_pids[@]}"
do
    wait "$pid"
done
writer_pids=()

$CLICKHOUSE_CLIENT -q "
    SELECT count(), min(version), max(version), uniqExact(payload)
    FROM $table
    WHERE bucket = 1 AND key < 5000"

for writer in $(seq 1 10)
do
    bucket=$((writer + 1))
    offset=$((40000 + writer * 1000))
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $table
        SELECT number + $offset, $bucket, 1, $writer
        FROM numbers(1000)" >/dev/null &
    writer_pids+=("$!")
done
for pid in "${writer_pids[@]}"
do
    wait "$pid"
done
writer_pids=()

$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(bucket), uniqExact(payload) FROM $table WHERE bucket IN (2,3,4,5,6,7,8,9,10,11)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 12, 12 FROM numbers(40000)" >/dev/null &
writer_pids+=("$!")
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_commit PAUSE"

for reader in $(seq 1 32)
do
    $CLICKHOUSE_CLIENT -q "
        SELECT sum(version)
        FROM $table
        WHERE bucket = 1
        SETTINGS max_threads = 4, max_block_size = 8192" > "$reader_dir/$reader" &
    reader_pids+=("$!")
done
for pid in "${reader_pids[@]}"
do
    wait "$pid"
done
reader_pids=()

if grep -Lx '90000' "$reader_dir"/* | grep -q .
then
    echo "reader observed a mixed generation"
else
    echo "readers kept one snapshot"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_before_commit"
for pid in "${writer_pids[@]}"
do
    wait "$pid"
done
writer_pids=()
$CLICKHOUSE_CLIENT -q "SELECT sum(version), uniqExact(version) FROM $table WHERE bucket = 1"

if $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $bad_table (key UInt64, version UInt64)
    ENGINE = OverwriteCache(version)
    KEYS (key)
    SETTINGS max_memory_bytes = 1000000, max_concurrent_insert_preparations = 0" >/dev/null 2>&1
then
    echo "invalid admission setting accepted"
else
    echo "invalid admission setting rejected"
fi

if $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $bad_table (key UInt64, version UInt64)
    ENGINE = OverwriteCache(version)
    KEYS (key)
    SETTINGS max_memory_bytes = 1000000, max_insert_publication_threads = 0" >/dev/null 2>&1
then
    echo "invalid publication thread setting accepted"
else
    echo "invalid publication thread setting rejected"
fi
