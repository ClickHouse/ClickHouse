#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src_123;
DROP TABLE IF EXISTS mv_123;

CREATE TABLE src_123 (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_123 (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src_123;
EOF

STOP_ALTER=0
total_alter_time=0
alter_count=0
total_sleep_time=0
sleep_count=0

function alter_thread()
{
    trap 'STOP_ALTER=1' SIGINT SIGTERM

    ALTERS[0]="ALTER TABLE mv_123 MODIFY QUERY SELECT v FROM src_123;"
    ALTERS[1]="ALTER TABLE mv_123 MODIFY QUERY SELECT v * 2 as v FROM src_123;"

    while [[ $STOP_ALTER -eq 0 ]]; do
        start_alter=$(date +%s%3N)
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        end_alter=$(date +%s%3N)

        duration_alter=$((end_alter - start_alter))
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] [ALTER] took ${duration_alter}ms"

        total_alter_time=$((total_alter_time + duration_alter))
        alter_count=$((alter_count + 1))

        start_sleep=$(date +%s%3N)
        sleep "$(echo 0.$RANDOM)"
        end_sleep=$(date +%s%3N)

        duration_sleep=$((end_sleep - start_sleep))
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] [SLEEP] took ${duration_sleep}ms"

        total_sleep_time=$((total_sleep_time + duration_sleep))
        sleep_count=$((sleep_count + 1))
    done

    echo
    echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] ==================== ALTER Thread Stats ===================="
    if [[ $alter_count -gt 0 ]]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] ALTER Count: $alter_count, Avg Duration: $((total_alter_time / alter_count)) ms"
    else
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] No ALTER executed."
    fi
    if [[ $sleep_count -gt 0 ]]; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] SLEEP Count: $sleep_count, Avg Duration: $((total_sleep_time / sleep_count)) ms"
    else
        echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] No sleep executed."
    fi
    echo "[$(date "+%Y-%m-%d %H:%M:%S.%3N")] ============================================================"
}

export -f alter_thread;

timeout 10 bash -c alter_thread &
ALTER_PID=$!

total_insert_time=0
insert_count=0

for i in {1..100}; do
    while true; do
        start_insert=$(date +%s%3N)
        if $CLICKHOUSE_CLIENT -q "INSERT INTO src_123 VALUES (1);" 2>/dev/null; then
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

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_123;"

wait $ALTER_PID || true

$CLICKHOUSE_CLIENT -q "DROP VIEW mv_123"
$CLICKHOUSE_CLIENT -q "DROP TABLE src_123"
