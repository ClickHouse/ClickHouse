#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_insert_storage_snapshot"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_insert_storage_snapshot (a UInt64) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_insert_storage_snapshot VALUES (1)"

query_id="$CLICKHOUSE_DATABASE-$RANDOM"
$CLICKHOUSE_CLIENT --query_id $query_id -q "INSERT INTO t_insert_storage_snapshot SELECT sleep(1) FROM numbers(1000) SETTINGS max_block_size = 1" 2>/dev/null &

$CLICKHOUSE_CLIENT -q "SELECT name, active, refcount FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_insert_storage_snapshot'"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" >/dev/null

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_insert_storage_snapshot"
