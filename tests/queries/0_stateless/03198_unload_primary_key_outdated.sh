#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT "
    DROP TABLE IF EXISTS t_unload_primary_key;

    CREATE TABLE t_unload_primary_key (a UInt64, b UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS old_parts_lifetime = 10000;

    INSERT INTO t_unload_primary_key VALUES (1, 1);

    SELECT name, active, primary_key_bytes_in_memory FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_unload_primary_key' ORDER BY name;

    ALTER TABLE t_unload_primary_key UPDATE b = 100 WHERE 1 SETTINGS mutations_sync = 2;
"

for _ in {1..100}; do
    res=$($CLICKHOUSE_CLIENT -q "SELECT primary_key_bytes_in_memory FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_unload_primary_key' AND name = 'all_1_1_0'")
    if [[ $res -eq 0 ]]; then
        break
    fi
    sleep 0.3
done

$CLICKHOUSE_CLIENT "
    SELECT name, active, primary_key_bytes_in_memory FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_unload_primary_key' ORDER BY name;
    DROP TABLE IF EXISTS t_unload_primary_key;
"
