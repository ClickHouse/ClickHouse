#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In this test we only increase cache size and then decrease it back while making concurrent selects.
# There is no .reference file, because the purpose of this test
# is to be run in parallel to other tests, not to produce .reference.
# `s3_cache` is used for many tests and for all tests in case of s3-storage run.
disk_name="s3_cache"
table_name="table_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS ${table_name};
CREATE TABLE ${table_name} (a String) engine=MergeTree() ORDER BY tuple() SETTINGS disk = '$disk_name';
INSERT INTO ${table_name} SELECT randomString(10000000);
"

$CLICKHOUSE_CLIENT --query "SELECT * FROM ${table_name} FORMAT Null"

prev_max_size=$($CLICKHOUSE_CLIENT --query "SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
$CLICKHOUSE_CLIENT --query "SELECT current_size > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name' FORMAT TabSeparated"

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/storage_conf.xml

new_max_size=$($CLICKHOUSE_CLIENT --query "SELECT multiply(max_size, 3) FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'")
sed -i "s|<max_size>$prev_max_size<\/max_size>|<max_size>$new_max_size<\/max_size>|"  $config_path

TIMEOUT=5

function select_func {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM ${table_name} FORMAT Null SETTINGS filesystem_cache_segments_batch_size=1, max_read_buffer_size_remote_fs=50000"
    done
}

select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &

$CLICKHOUSE_CLIENT -m --query "
SET send_logs_level='error';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT sleep(3) FORMAT Null"
wait

sed -i "s|<max_size>$new_max_size<\/max_size>|<max_size>$prev_max_size<\/max_size>|"  $config_path

select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &
select_func 2>/dev/null &

$CLICKHOUSE_CLIENT -m --query "
SET send_logs_level='error';
SYSTEM RELOAD CONFIG"

$CLICKHOUSE_CLIENT --query "SELECT sleep(3) FORMAT Null"
wait
