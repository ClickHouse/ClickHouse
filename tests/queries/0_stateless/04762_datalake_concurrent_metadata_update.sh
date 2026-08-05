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
trap 'rm -rf "${DELTA_DIR:?}"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

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

# The probe above cached delta-kernel metadata, whose supportsUpdate() is true, so further
# queries refresh it in place instead of reassigning the pointer. Re-creating under the
# kernel-disabled setting caches metadata that has to be replaced instead.
$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 --multiquery "$CREATE"

# The kernel-enabled readers below then replace that pointer concurrently while reading it.
# One long-lived client per stream keeps the queries back to back.
READS="SELECT total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() FORMAT Null;"
COUNTS="SELECT count() FROM ${TABLE} FORMAT Null;"
for _ in {1..4}; do
    yes "$READS" | head -n 40 |
        $CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 --multiquery >/dev/null 2>&1 &
    yes "$COUNTS" | head -n 40 |
        $CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=0 --multiquery >/dev/null 2>&1 &
done
wait

$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 \
    -q "SELECT total_rows > 0 FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"
