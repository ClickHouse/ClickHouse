#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="s3_cache"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a String) engine=MergeTree() ORDER BY tuple() SETTINGS disk = '$disk_name';
INSERT INTO test SELECT randomString(1000);
"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

prev_max_size=$($CLICKHOUSE_CLIENT --query "SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
$CLICKHOUSE_CLIENT --query "SELECT current_size > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml

new_max_size=$($CLICKHOUSE_CLIENT --query "SELECT divide(max_size, 2) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
sed -i "s|<max_size>$prev_max_size<\/max_size>|<max_size>$new_max_size<\/max_size>|"  $config_path

# echo $prev_max_size
# echo $new_max_size

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT max_size == $new_max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT current_size > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT current_size <= max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

sed -i "s|<max_size>$new_max_size<\/max_size>|<max_size>$prev_max_size<\/max_size>|"  $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT max_size == $prev_max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT current_size > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT current_size <= max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
