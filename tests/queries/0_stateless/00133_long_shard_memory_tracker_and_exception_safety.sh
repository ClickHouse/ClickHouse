#!/usr/bin/env bash
# Tags: long, shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS numbers_100k;
    CREATE VIEW numbers_100k AS SELECT * FROM system.numbers LIMIT 100000;
";

STEP_MULTIPLIER=25

# Pin `max_threads=1`: the server-level `additional_memory_tracking_per_thread`
# speculatively reserves 4 MiB per pipeline-executor thread, so with the default
# (core-count) `max_threads` the reservation alone (cores * 4 MiB, on every shard)
# dwarfs the `max_memory_usage` values swept here and the query never succeeds.
# With a single thread the reservation is a fixed 4 MiB and the sweep behaves as
# before, while still exercising memory-limit exception safety.
for i in $(seq 1000000 $((20000 * $STEP_MULTIPLIER)) 10000000 && seq 10100000 $((100000 * $STEP_MULTIPLIER)) 50000000); do
    $CLICKHOUSE_CLIENT --max_threads=1 --max_memory_usage="$i" --max_bytes_before_external_group_by 0 --max_bytes_ratio_before_external_group_by 0 --query="
        SELECT intDiv(number, 5) AS k, max(toString(number)) FROM remote('127.0.0.{2,3}', ${CLICKHOUSE_DATABASE}.numbers_100k) GROUP BY k ORDER BY k LIMIT 1;
    " 2> "${CLICKHOUSE_TMP}/err";
    CODE=$?;
    [ "$CODE" -ne "241" ] && [ "$CODE" -ne "0" ] && echo "Fail, Code: ${CODE}" && cat "${CLICKHOUSE_TMP}/err" && break;
done | uniq

$CLICKHOUSE_CLIENT --query="DROP TABLE numbers_100k;";
