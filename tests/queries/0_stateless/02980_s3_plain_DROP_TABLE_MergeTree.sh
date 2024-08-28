#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# Tag no-fasttest: requires S3
# Tag no-random-settings, no-random-merge-tree-settings: to avoid creating extra files like serialization.json, this test too exocit anyway

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# config for clickhouse-disks (to check leftovers)
config="${BASH_SOURCE[0]/.sh/.yml}"

# only in Atomic ATTACH from s3_plain works
new_database="ordinary_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "create database $new_database engine=Ordinary"
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$new_database}
CLICKHOUSE_DATABASE="$new_database"

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by key;
    insert into data values (1);
    select 'data after INSERT', count() from data;
"

# suppress output
$CLICKHOUSE_CLIENT -q "backup table data to S3('http://localhost:11111/test/s3_plain/backups/$CLICKHOUSE_DATABASE', 'test', 'testtest')" > /dev/null

$CLICKHOUSE_CLIENT -nm -q "
    drop table data;
    attach table data (key Int) engine=MergeTree() order by key
    settings
        max_suspicious_broken_parts=0,
        disk=disk(type=s3_plain,
            endpoint='http://localhost:11111/test/s3_plain/backups/$CLICKHOUSE_DATABASE',
            access_key_id='test',
            secret_access_key='testtest');
    select 'data after ATTACH', count() from data;

    insert into data values (1); -- { serverError TABLE_IS_READ_ONLY }
    optimize table data final; -- { serverError TABLE_IS_READ_ONLY }
"

path=$($CLICKHOUSE_CLIENT -q "SELECT replace(data_paths[1], 's3_plain', '') FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'data'")
# trim / to fix "Unable to parse ExceptionName: XMinioInvalidObjectName Message: Object name contains unsupported characters."
path=${path%/}

echo "Files before DETACH TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "${path:?}" | tail -n+2

$CLICKHOUSE_CLIENT -q "detach table data"
echo "Files after DETACH TABLE"
clickhouse-disks -C "$config" --disk s3_plain_disk list --recursive "$path" | tail -n+2

# metadata file is left
$CLICKHOUSE_CLIENT --force_remove_data_recursively_on_drop=1 -q "drop database if exists $CLICKHOUSE_DATABASE"
