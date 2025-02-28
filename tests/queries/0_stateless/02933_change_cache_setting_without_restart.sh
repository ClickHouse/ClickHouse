#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="s3_cache_02933"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml
config_path_tmp=$config_path.tmp

cat $config_path \
| sed "s|<background_download_threads>0<\/background_download_threads>|<background_download_threads>10<\/background_download_threads>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

cat $config_path \
| sed "s|<background_download_queue_size_limit>0<\/background_download_queue_size_limit>|<background_download_queue_size_limit>1000<\/background_download_queue_size_limit>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

cat $config_path \
| sed "s|<background_download_threads>10<\/background_download_threads>|<background_download_threads>5<\/background_download_threads>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

cat $config_path \
| sed "s|<background_download_threads>5<\/background_download_threads>|<background_download_threads>15<\/background_download_threads>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

cat $config_path \
| sed "s|<background_download_threads>15<\/background_download_threads>|<background_download_threads>2<\/background_download_threads>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

cat $config_path \
| sed "s|<background_download_threads>2<\/background_download_threads>|<background_download_threads>0<\/background_download_threads>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"

cat $config_path \
| sed "s|<background_download_queue_size_limit>1000<\/background_download_queue_size_limit>|<background_download_queue_size_limit>0<\/background_download_queue_size_limit>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"
