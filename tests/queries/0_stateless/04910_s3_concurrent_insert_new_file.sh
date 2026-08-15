#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

filename="test_04910_${CLICKHOUSE_DATABASE}_${RANDOM}"
writers=8

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04910"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04910 (a UInt64) ENGINE = S3(s3_conn, filename='$filename', format=Parquet)"

# The next file name is derived from the path list, so the base object must exist for a new one
# to be derived at all.
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 -q "INSERT INTO t_04910 SELECT 0"

# Readers overlap the writer burst so that list reads run concurrently with list writes.
# They assert nothing: a path becomes visible before its object is uploaded, so any row count
# here would be legitimately racy. Their only job is to touch the list concurrently.
# A fixed iteration count, not a loop until the writers finish: a reader that outlives its
# database would keep querying a dropped one.
reader_pids=()
for _ in 1 2; do
    (
        for _ in $(seq 1 10); do
            $CLICKHOUSE_CLIENT --s3_ignore_file_doesnt_exist 1 \
                -q "SELECT count() FROM t_04910" > /dev/null 2>&1 || true
            $CLICKHOUSE_CLIENT --s3_ignore_file_doesnt_exist 1 \
                -q "SELECT count() FROM t_04910 WHERE _file LIKE '%1'" > /dev/null 2>&1 || true
        done
    ) &
    reader_pids+=("$!")
done

writer_pids=()
for i in $(seq 1 $((writers - 1))); do
    $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_04910 SELECT $i" &
    writer_pids+=("$!")
done

# One writer goes through the asynchronous insert queue, which is the thread pairing this test is
# about. VALUES, not SELECT: only an inlined payload is queued rather than run synchronously.
# Exactly one such writer, because several would be batched into a single file and the
# distinct-file count below would stop being deterministic.
$CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 --async_insert 1 --wait_for_async_insert 1 \
    -q "INSERT INTO t_04910 VALUES ($writers)" &
writer_pids+=("$!")

wait "${writer_pids[@]}"
wait "${reader_pids[@]}" 2>/dev/null || true

# Every writer must have landed in a file of its own: a name derived twice means one writer
# overwrote another.
$CLICKHOUSE_CLIENT -q "SELECT count() = $writers + 1, countDistinct(_file) = $writers + 1 FROM t_04910"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(a) = range(toUInt64($writers + 1)) FROM (SELECT a FROM t_04910 ORDER BY a)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04910"
