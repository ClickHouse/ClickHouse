#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src;
EOF

STOP_ALTER=0

# 用于统计
total_alter_time=0
alter_count=0
total_sleep_time=0
sleep_count=0

function alter_thread()
{
    trap 'STOP_ALTER=1' SIGINT SIGTERM

    ALTERS[0]="ALTER TABLE mv MODIFY QUERY SELECT v FROM src;"
    ALTERS[1]="ALTER TABLE mv MODIFY QUERY SELECT v * 2 as v FROM src;"

    while [[ $STOP_ALTER -eq 0 ]]; do
        start_alter=$(date +%s%3N)
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        end_alter=$(date +%s%3N)
        dur_alter=$((end_alter - start_alter))

        echo "[ALTER] took ${dur_alter} ms"
        total_alter_time=$((total_alter_time + dur_alter))
        alter_count=$((alter_count + 1))

        start_sleep=$(date +%s%3N)
        sleep "$(echo 0.$RANDOM)";
        end_sleep=$(date +%s%3N)
        dur_sleep=$((end_sleep - start_sleep))

        echo "[SLEEP] took ${dur_sleep} ms"
        total_sleep_time=$((total_sleep_time + dur_sleep))
        sleep_count=$((sleep_count + 1))
    done

    # Insert done, drop tables
    $CLICKHOUSE_CLIENT -q "DROP VIEW mv"
    $CLICKHOUSE_CLIENT -q "DROP TABLE src"

    # print
    echo "=== ALTER THREAD STATS ==="
    if [[ $alter_count -gt 0 ]]; then
        echo "Total ALTERs: $alter_count, Average ALTER time: $((total_alter_time / alter_count)) ms"
    else
        echo "No ALTER executed"
    fi
    if [[ $sleep_count -gt 0 ]]; then
        echo "Total SLEEPs: $sleep_count, Average SLEEP time: $((total_sleep_time / sleep_count)) ms"
    else
        echo "No SLEEP executed"
    fi
    echo "========================="
}

export -f alter_thread;
alter_thread &
ALTER_DROP_PID=$!

for i in {1..100}; do
    while true; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1);" 2>/dev/null && break
    done
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv;"

kill -SIGINT $ALTER_DROP_PID
wait $ALTER_DROP_PID
