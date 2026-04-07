#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "
SELECT
    volume_name,
    volume_priority
FROM system.storage_policies
WHERE policy_name = 'policy_02961'
ORDER BY volume_priority ASC;
"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf_02961.xml
config_path_tmp=$config_path.tmp

echo 'check non-unique values dont work'
cat $config_path \
| sed "s|<volume_priority>2<\/volume_priority>|<volume_priority>1<\/volume_priority>|" \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='error';
SYSTEM RELOAD CONFIG" 2>&1 | grep -c 'volume_priority values must be unique across the policy'

#first, restore original values
cat $config_path \
| sed '0,/<volume_priority>1<\/volume_priority>/s//<volume_priority>2<\/volume_priority>/' \
> $config_path_tmp
mv $config_path_tmp $config_path

echo 'check no gaps in range allowed'
cat $config_path \
| sed '0,/<volume_priority>1<\/volume_priority>/s//<volume_priority>3<\/volume_priority>/' \
> $config_path_tmp
mv $config_path_tmp $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='error';
SYSTEM RELOAD CONFIG" 2>&1 | grep -c 'volume_priority values must cover the range from 1 to N (lowest priority specified) without gaps'

echo 'restore valid config'
cat $config_path \
| sed '0,/<volume_priority>3<\/volume_priority>/s//<volume_priority>1<\/volume_priority>/' \
> $config_path_tmp
mv $config_path_tmp $config_path
