#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=test_02497_$CLICKHOUSE_TEST_UNIQUE_NAME.tsv
echo -e 'key\nfoo\nbar' > $DATA_FILE

$CLICKHOUSE_LOCAL --storage_file_read_method=mmap --print-profile-events -q "SELECT * FROM file($DATA_FILE) FORMAT Null" 2>&1 | grep -F -c "CreatedReadBufferMMap:"
$CLICKHOUSE_LOCAL --storage_file_read_method=mmap --print-profile-events -q "SELECT * FROM file($DATA_FILE) FORMAT Null" 2>&1 | grep -F -c "CreatedReadBufferOrdinary"

$CLICKHOUSE_LOCAL --storage_file_read_method=pread --print-profile-events -q "SELECT * FROM file($DATA_FILE) FORMAT Null" 2>&1 | grep -F -c "CreatedReadBufferMMap"
$CLICKHOUSE_LOCAL --storage_file_read_method=pread --print-profile-events -q "SELECT * FROM file($DATA_FILE) FORMAT Null" 2>&1 | grep -F -c "CreatedReadBufferOrdinary"

rm $DATA_FILE
