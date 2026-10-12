#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergS3(s3_conn, filename = 'issue87414/test/t0') SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json'"
$CLICKHOUSE_CLIENT -q "SELECT count(*), sum(c0) FROM ${TABLE}"

# Re-create without iceberg_metadata_file_path so that the write retry
# mechanism can discover the actual latest metadata version via directory
# scan, avoiding infinite retries when v3 already exists from a concurrent
# or previous run.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergS3(s3_conn, filename = 'issue87414/test/t0')"
echo "INSERT INTO TABLE ${TABLE} (c0) SETTINGS write_full_path_in_iceberg_metadata = 1, allow_insert_into_iceberg=1 VALUES (1)" | $CLICKHOUSE_CLIENT 2>/dev/null

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergS3(s3_conn, filename = 'issue87414/test/t0') SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json'"
$CLICKHOUSE_CLIENT -q "SELECT count(*), sum(c0) FROM ${TABLE}"
