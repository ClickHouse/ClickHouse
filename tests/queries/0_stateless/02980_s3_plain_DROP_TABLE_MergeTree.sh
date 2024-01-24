#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${BASH_SOURCE[0]/.sh/.yml}"

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by key settings disk='s3_plain_disk';
    insert into data values (1); -- { serverError TABLE_IS_READ_ONLY }
    optimize table data final; -- { serverError TABLE_IS_READ_ONLY }
"

path=$($CLICKHOUSE_CLIENT -q "SELECT replace(data_paths[1], 's3_plain', '') FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'data'")
# trim / to fix "Unable to parse ExceptionName: XMinioInvalidObjectName Message: Object name contains unsupported characters."
path=${path%/}

echo "Files before DROP TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "${path:?}" | tail -n+2

$CLICKHOUSE_CLIENT -q "detach table data"
echo "Files after DETACH TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "$path" | tail -n+2

$CLICKHOUSE_CLIENT -nm -q "
    attach table data;
    drop table data;
"
# Check that there is no leftovers:
echo "Files after DROP TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "$path" | tail -n+2
