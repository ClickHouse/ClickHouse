#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="s3_cache_02933"
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml

sed -i "s|<background_download_threads>0<\/background_download_threads>|<background_download_threads>10<\/background_download_threads>|" $config_path
sed -i "s|<background_download_queue_size_limit>0<\/background_download_queue_size_limit>|<background_download_queue_size_limit>1000<\/background_download_queue_size_limit>|" $config_path

# In case of listen_try we can have 'Address already in use'
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

sed -i "s|<background_download_threads>10<\/background_download_threads>|<background_download_threads>5<\/background_download_threads>|" $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

sed -i "s|<background_download_threads>5<\/background_download_threads>|<background_download_threads>15<\/background_download_threads>|" $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

sed -i "s|<background_download_threads>15<\/background_download_threads>|<background_download_threads>2<\/background_download_threads>|" $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

sed -i "s|<background_download_threads>2<\/background_download_threads>|<background_download_threads>0<\/background_download_threads>|" $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"

sed -i "s|<background_download_queue_size_limit>1000<\/background_download_queue_size_limit>|<background_download_queue_size_limit>0<\/background_download_queue_size_limit>|" $config_path

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
$CLICKHOUSE_CLIENT --query "select background_download_threads, background_download_queue_size_limit from system.filesystem_cache_settings where cache_name = '${disk_name}'"
