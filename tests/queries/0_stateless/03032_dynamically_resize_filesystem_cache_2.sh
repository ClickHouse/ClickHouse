#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="dynamically_resize_filesystem_cache"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a String) engine=MergeTree() ORDER BY tuple()
SETTINGS disk = 'dynamically_resize_filesystem_cache';
"

query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-query"
# Write-through cache is enabled.
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "INSERT INTO test SELECT randomString(10000) SETTINGS enable_filesystem_cache_on_write_operations=1;"

min_expected_size=2000

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log;"

written_cache=$($CLICKHOUSE_CLIENT --query "SELECT ProfileEvents['CachedWriteBufferCacheWriteBytes'] FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'")

$CLICKHOUSE_CLIENT --query "SELECT if($written_cache > $min_expected_size, 'written cache is bigger than $min_expected_size', 'written_cache: $written_cache') FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT if(current_size > $min_expected_size, 'current size after insert is bigger than $min_expected_size', concat('current_size: ', current_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

prev_max_size=$($CLICKHOUSE_CLIENT --query "SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml

# Choose a size of cache to which it will be resized.
# In config for 'dynamically_resize_filesystem_cache' cache: max_size=~2k, max_file_segment_size=100, cache_policy=LRU.
# We need to guarantee that the cache will contain at least this amount of bytes.
new_max_size=$($CLICKHOUSE_CLIENT --query "SELECT divide(max_size, 3) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")

$CLICKHOUSE_CLIENT --query "SELECT if($written_cache > $new_max_size, 'written cache is bigger than $new_max_size', 'written_cache: $written_cache') FORMAT TabSeparated"
# Check that cache currently holds more data than the new (smaller) max_size, before any resize.
$CLICKHOUSE_CLIENT --query "SELECT if(current_size > $new_max_size, 'current size is bigger than $new_max_size', concat('current_size: ', current_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

# Modify config to reduce max_size. Note: ConfigReloader may auto-detect this change and resize the cache
# before we call SYSTEM RELOAD CONFIG, so all checks above must happen before this sed.
sed -i "s|<max_size>$prev_max_size<\/max_size>|<max_size>$new_max_size<\/max_size>|" $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT if(max_size == $new_max_size, 'max size is changed', concat('Expected: $new_max_size, Actual: ', max_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT if(current_size > 0, 'current size is still non-zero', concat('current_size: ', current_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT if(current_size <= max_size, 'current size is less than max size', concat('current_size: ', current_size, ', max_size: ', max_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

sed -i "s|<max_size>$new_max_size<\/max_size>|<max_size>$prev_max_size<\/max_size>|" $config_path

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT if(max_size == $prev_max_size, 'max size is reloaded', concat('Expected: $prev_max_size, Actual: ', max_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT if(current_size > 0, 'current size is still non-zero', concat('current_size: ', current_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT --query "SELECT if(current_size <= max_size, 'current size is less than max size', concat('current_size: ', current_size, ', max_size: ', max_size)) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"
