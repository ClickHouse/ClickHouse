#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# Concurrent readers of the same DeltaLake table while its metadata is republished.
# A data race is not visible in query output, so a regression shows up as a sanitizer
# report rather than as a diff against the reference.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_concurrent"
TABLE="t_${CLICKHOUSE_DATABASE}"
OUT_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_streams"
trap 'rm -rf "${DELTA_DIR:?}" "${OUT_DIR:?}"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

rm -rf "${DELTA_DIR:?}"
mkdir -p "$DELTA_DIR"
cp -r "$CUR_DIR"/data_delta_lake/lakehouses/spark_catalog/test/t0/. "$DELTA_DIR/"

CREATE="DROP TABLE IF EXISTS ${TABLE};
        CREATE TABLE ${TABLE} ENGINE = DeltaLakeLocal('${DELTA_DIR}')"
$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 --multiquery "$CREATE"

# total_rows is served only when the delta kernel is enabled.
$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 \
    -q "SELECT total_rows IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"
$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 \
    -q "SELECT total_rows IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"

# The probe above cached delta-kernel metadata, whose `supportsUpdate` is true, so further
# queries refresh it in place instead of reassigning the pointer. Re-creating under the
# kernel-disabled setting caches metadata that has to be replaced instead.
$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 --multiquery "$CREATE"

# The kernel-enabled readers below then replace that pointer concurrently while reading it.
# One long-lived client per stream keeps the queries back to back. Each iteration ends with a
# marker: the race is invisible in query output, so without counting the queries a stream
# really issued, one dying early would still match the reference.
# The kernel-disabled stream must not run a read pipeline: `createFileIterator` reads the metadata
# twice, so it can pin no snapshot version from the object the first read saw and then call
# `iterate` on a replacement that requires one. DESCRIBE reaches `update` without that iterator.
STREAMS=4
QUERIES=12
READS="SELECT total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() FORMAT Null; SELECT 1 FORMAT TSVRaw;"
COUNTS="DESCRIBE TABLE ${TABLE} FORMAT Null; SELECT 1 FORMAT TSVRaw;"
rm -rf "${OUT_DIR:?}"
mkdir -p "$OUT_DIR"
pids=()
for i in $(seq 1 $STREAMS); do
    yes "$READS" | head -n $QUERIES |
        $CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 --multiquery > "$OUT_DIR/reads-$i" 2>/dev/null &
    pids+=($!)
    yes "$COUNTS" | head -n $QUERIES |
        $CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 --multiquery > "$OUT_DIR/counts-$i" 2>/dev/null &
    pids+=($!)
done
# Bare `wait` reports success once every child is reaped, whatever they exited with.
for pid in "${pids[@]}"; do
    wait "$pid" || echo "concurrent stream $pid failed with status $?"
done

echo "$(cat "$OUT_DIR"/reads-* | wc -l) $(cat "$OUT_DIR"/counts-* | wc -l)"

$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 \
    -q "SELECT total_rows > 0 FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"
