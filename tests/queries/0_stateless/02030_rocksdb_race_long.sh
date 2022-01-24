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
    while true; do
        echo "
            SELECT * FROM system.rocksdb FORMAT Null;
        " | $CLICKHOUSE_CLIENT -n
    done
}

function truncate_thread()
{
    while true; do
        sleep 3s;
        echo "
            TRUNCATE TABLE rocksdb_race;
        " | $CLICKHOUSE_CLIENT -n
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f read_stat_thread;
export -f truncate_thread;

TIMEOUT=20

timeout $TIMEOUT bash -c read_stat_thread 2> /dev/null &
timeout $TIMEOUT bash -c truncate_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE rocksdb_race"
