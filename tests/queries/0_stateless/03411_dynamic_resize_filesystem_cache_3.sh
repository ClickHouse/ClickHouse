#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In this test we only increase cache size two times and then decrease it back.
# There is no .reference file, because the purpose of this test
# is to be run in parallel to other tests, not to produce .reference.
# `s3_cache` is used for many tests and for all tests in case of s3-storage run.
disk_name="s3_cache"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a String) engine=MergeTree() ORDER BY tuple() SETTINGS disk = '$disk_name';
INSERT INTO test SELECT randomString(10000000);
"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

prev_max_size=$($CLICKHOUSE_CLIENT --query "SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
$CLICKHOUSE_CLIENT --query "SELECT current_size > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

#config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml
config_path=~/workspace/ClickHouse2/programs/server/config.xml

new_max_size=$($CLICKHOUSE_CLIENT --query "SELECT multiply(max_size, 5) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
sed -i "s|<max_size>$prev_max_size<\/max_size>|<max_size>$new_max_size<\/max_size>|"  $config_path

function select {
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"
    done
}

export -f select
timeout 5 bash -c select 2>/dev/null &
timeout 5 bash -c select 2>/dev/null &
timeout 5 bash -c select 2>/dev/null &
timeout 5 bash -c select 2>/dev/null &
timeout 5 bash -c select 2>/dev/null &

$CLICKHOUSE_CLIENT -m --query "
set send_logs_level='fatal';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT sleep(3) FORMAT Null"
wait

new_max_size=$($CLICKHOUSE_CLIENT --query "SELECT divide(max_size, 2) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
sed -i "s|<max_size>$prev_max_size<\/max_size>|<max_size>$new_max_size<\/max_size>|"  $config_path

