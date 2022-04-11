#!/usr/bin/env bash
# Tags: race

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

echo "
    DROP TABLE IF EXISTS rocksdb_race;
    CREATE TABLE rocksdb_race (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key);
    INSERT INTO rocksdb_race SELECT '1_' || toString(number), number FROM numbers(100000);
" | $CLICKHOUSE_CLIENT -n

function read_stat_thread()
{
    $CLICKHOUSE_CLIENT -n -q 'SELECT * FROM system.rocksdb FORMAT Null'
}

function truncate_thread()
{
    sleep 3s;
    $CLICKHOUSE_CLIENT -n -q 'TRUNCATE TABLE rocksdb_race'
}

export -f read_stat_thread
export -f truncate_thread

TIMEOUT=20

clickhouse_client_loop_timeout $TIMEOUT read_stat_thread 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT truncate_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE rocksdb_race"
