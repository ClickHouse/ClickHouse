#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src_456;
DROP TABLE IF EXISTS mv_456;

CREATE TABLE src_456 (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_456 (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src_456;
EOF

STOP_ALTER=0

total_alter_time=0
alter_count=0
total_sleep_time=0
sleep_count=0

function alter_thread()
{
    trap 'STOP_ALTER=1' SIGINT SIGTERM

    ALTERS[0]="ALTER TABLE mv_456 MODIFY QUERY SELECT v FROM src_456;"
    ALTERS[1]="ALTER TABLE mv_456 MODIFY QUERY SELECT v * 2 as v FROM src_456;"

    while [[ $STOP_ALTER -eq 0 ]]; do
        start_alter=$(date +%s%3N)
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        end_alter=$(date +%s%3N)
        dur_alter=$((end_alter - start_alter))

        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")][ALTER] took ${dur_alter} ms"
        total_alter_time=$((total_alter_time + dur_alter))
        alter_count=$((alter_count + 1))

        start_sleep=$(date +%s%3N)
        sleep "$(echo 0.$RANDOM)";
        end_sleep=$(date +%s%3N)
        dur_sleep=$((end_sleep - start_sleep))

        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")][SLEEP]  took ${dur_sleep} ms"
        total_sleep_time=$((total_sleep_time + dur_sleep))
        sleep_count=$((sleep_count + 1))
    done

    # Insert done, drop tables
    $CLICKHOUSE_CLIENT -q "DROP VIEW mv_456"
    $CLICKHOUSE_CLIENT -q "DROP TABLE src_456"

    # print
    echo "=== ALTER THREAD STATS ==="
    if [[ $alter_count -gt 0 ]]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] Total ALTERs: $alter_count, Average ALTER time: $((total_alter_time / alter_count)) ms"
    else
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] No ALTER executed"
    fi
    if [[ $sleep_count -gt 0 ]]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] Total SLEEPs: $sleep_count, Average SLEEP time: $((total_sleep_time / sleep_count)) ms"
    else
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] No SLEEP executed"
    fi
    echo "========================="
}

export -f alter_thread;
alter_thread &
ALTER_DROP_PID=$!
total_insert_time=0
insert_count=0
for i in {1..100}; do
    start_insert=$(date +%s%3N)
    while true; do
        if $CLICKHOUSE_CLIENT -q "INSERT INTO src_456 VALUES (1);" 2>/dev/null; then
            end_insert=$(date +%s%3N)
            duration_insert=$((end_insert - start_insert))
            echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] [INSERT #$i] took ${duration_insert}ms"

            total_insert_time=$((total_insert_time + duration_insert))
            insert_count=$((insert_count + 1))
            break
        fi
    done
done

if [[ $insert_count -gt 0 ]]; then
    echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] ==================== INSERT Stats ===================="
    echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] INSERT Count: $insert_count, Avg Duration: $((total_insert_time / insert_count)) ms"
    echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] ======================================================"
fi

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_456;"

kill -SIGINT $ALTER_DROP_PID
wait $ALTER_DROP_PID
