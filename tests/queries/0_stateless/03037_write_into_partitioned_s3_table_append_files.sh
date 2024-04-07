#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

filename="${CLICKHOUSE_DATABASE}_${RANDOM}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_s3_partition_by_append_03036"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_s3_partition_by_append_03036 (a UInt64) ENGINE = S3(s3_conn, filename = '${filename}_{_partition_id}', format = Parquet) PARTITION BY a"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_s3_partition_by_append_03036 SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_s3_partition_by_append_03036 SELECT number FROM numbers(10) SETTINGS s3_create_new_file_on_insert=1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM s3(s3_conn, filename = '${filename}_*', format = Parquet, structure = 'a UInt64') SETTINGS max_threads=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_s3_partition_by_append_03036"
