#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

allowed_name_length=$($CLICKHOUSE_CLIENT -mn --query="SELECT getMaxTableNameLengthForDatabase('$CLICKHOUSE_DATABASE')")
excess_length=$((allowed_name_length + 1))  # Ensure exceeding the limit
long_table_name=$(openssl rand -base64 $((3 * excess_length)) | tr -dc A-Za-z | head -c $excess_length)
allowed_table_name=$(openssl rand -base64 $((3 * allowed_name_length)) | tr -dc A-Za-z | head -c $allowed_name_length)

$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $long_table_name (id UInt32, long_table_name String) Engine=MergeTree() order by id;" 2>&1 | grep -o -m 1 'ARGUMENT_OUT_OF_BOUND'

use_s3_plain_rewriteable_as_db_disk=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" | tr -d '[:space:]') 
# When using s3_plain_rewriteable as a db_disk, minio doesn't allow the path segment to have more than 255 characters
# Refer: https://github.com/minio/minio/blob/ddd9a84cd769e6bed67f5fe860f8f3c7527a6971/cmd/xl-storage.go#L154-L167
if [ "$use_s3_plain_rewriteable_as_db_disk" == "0" ]; then
    $CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $allowed_table_name (id UInt32, allowed_table_name String) Engine=MergeTree() order by id;"
    $CLICKHOUSE_CLIENT -mn --query="DROP TABLE $allowed_table_name;"
fi
