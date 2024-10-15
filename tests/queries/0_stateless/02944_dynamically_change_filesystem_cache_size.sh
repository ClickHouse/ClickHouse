#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="s3_cache_02944"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a String) engine=MergeTree() ORDER BY tuple() SETTINGS disk = '$disk_name';
INSERT INTO test SELECT randomString(100);
SYSTEM DROP FILESYSTEM CACHE;
"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
$CLICKHOUSE_CLIENT --query "SELECT sum(size) FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf_02944.xml
config_path_tmp=$config_path.tmp

echo 'set max_size from 100 to 10'
cat $config_path \
| sed "s|<max_size>100<\/max_size>|<max_size>10<\/max_size>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
$CLICKHOUSE_CLIENT --query "SELECT sum(size) FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"

echo 'set max_size from 10 to 100'
cat $config_path \
| sed "s|<max_size>10<\/max_size>|<max_size>100<\/max_size>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
$CLICKHOUSE_CLIENT --query "SELECT sum(size) FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"

echo 'set max_elements from 10 to 2'
cat $config_path \
| sed "s|<max_elements>10<\/max_elements>|<max_elements>2<\/max_elements>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
$CLICKHOUSE_CLIENT --query "SELECT sum(size) FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"

echo 'set max_elements from 2 to 10'
cat $config_path \
| sed "s|<max_elements>2<\/max_elements>|<max_elements>10<\/max_elements>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
$CLICKHOUSE_CLIENT --query "SELECT sum(size) FROM system.filesystem_cache WHERE state = 'DOWNLOADED'"
