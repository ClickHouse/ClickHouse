#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: IcebergLocal needs Avro and Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_time/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_time"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

# Iceberg `time` is microseconds since midnight, which ClickHouse does not write, so no schema may declare it.
for columns in "id Int64, tm Time" "id Int64, tm Nullable(Time)" "id Int64, tm Array(Time)" \
               "id Int64, tm Map(String, Time)" "id Int64, tm Tuple(a Time)"
do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --enable_time_time64_type=1 \
        --query "CREATE TABLE t_time (${columns}) ENGINE = IcebergLocal('${TABLE_PATH}')" 2>&1 \
        | grep -o "Unsupported type for iceberg Time" | head -n1
done
# Nothing was written, so the path is still free for a table Iceberg can hold.
ls "${TABLE_PATH}" 2>/dev/null

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "CREATE TABLE t_time (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --enable_time_time64_type=1 \
    --query "ALTER TABLE t_time ADD COLUMN tm Nullable(Time)" 2>&1 \
    | grep -o "Unsupported type for iceberg Time" | head -n1
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE t_time ADD COLUMN c Nullable(Int64)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --enable_time_time64_type=1 \
    --query "ALTER TABLE t_time MODIFY COLUMN c Nullable(Time)" 2>&1 \
    | grep -o "Unsupported type for iceberg Time" | head -n1
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE t_time FORMAT TSV" | cut -f1,2
