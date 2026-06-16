#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

client_opts=(
  --allow_repeated_settings
  --send_logs_level 'error'
)

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into data select * from numbers(10);
"

$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data')"
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" -q "RESTORE TABLE data AS data_native_copy FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data')"
