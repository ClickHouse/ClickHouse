#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

filename="${CLICKHOUSE_DATABASE}_${RANDOM}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_s3_partition_by_file_exists_03036"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_s3_partition_by_file_exists_03036 (a UInt64) ENGINE = S3(s3_conn, filename = '${filename}_{_partition_id}', format = Parquet) PARTITION BY a"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_s3_partition_by_file_exists_03036 SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_s3_partition_by_file_exists_03036 SELECT number FROM numbers(10)"  2>&1 | grep --max-count 1 -o -F 'If you want to overwrite it, enable setting s3_truncate_on_insert, if you want to create a new file on each insert, enable setting s3_create_new_file_on_insert. (BAD_ARGUMENTS)'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_s3_partition_by_file_exists_03036"
