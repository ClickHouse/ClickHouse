#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${BASH_SOURCE[0]/.sh/.yml}"

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_read;
    drop table if exists data_write;

    create table data_read (key Int) engine=ReplicatedMergeTree('/tables/{database}/data', 'read') order by key settings disk='s3_plain_disk';
    create table data_write (key Int) engine=ReplicatedMergeTree('/tables/{database}/data', 'write') order by key;

    insert into data_write values (1);
    system sync replica data_read; -- { serverError TABLE_IS_READ_ONLY }
"

path=$($CLICKHOUSE_CLIENT -q "SELECT replace(data_paths[1], 's3_plain', '') FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'data_read'")
# trim / to fix "Unable to parse ExceptionName: XMinioInvalidObjectName Message: Object name contains unsupported characters."
path=${path%/}

echo "Files before DROP TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "${path:?}" | tail -n+2

$CLICKHOUSE_CLIENT -nm -q "
    detach table data_read;
    detach table data_write;
"
echo "Files after DETACH TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "$path" | tail -n+2

$CLICKHOUSE_CLIENT -nm -q "
    attach table data_read;
    attach table data_write;

    drop table data_read;
    drop table data_write;
"
# Check that there is no leftovers:
echo "Files after DETACH TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "$path" | tail -n+2
