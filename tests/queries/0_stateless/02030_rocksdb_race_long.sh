#!/usr/bin/env bash
# Tags: race, use-rocksdb

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

echo "
	DROP TABLE IF EXISTS rocksdb_race;
	CREATE TABLE rocksdb_race (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key);
    INSERT INTO rocksdb_race SELECT '1_' || toString(number), number FROM numbers(100000);
" | $CLICKHOUSE_CLIENT

function read_stat_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        echo "
            SELECT * FROM system.rocksdb FORMAT Null;
        " | $CLICKHOUSE_CLIENT
    done
}

function truncate_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        sleep 3s;
        echo "
            TRUNCATE TABLE rocksdb_race;
        " | $CLICKHOUSE_CLIENT
    done
}

TIMEOUT=20

read_stat_thread 2> /dev/null &
truncate_thread 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE rocksdb_race"
