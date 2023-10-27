#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


total_iterations=1000
iterations_in_parallel=8

$CLICKHOUSE_CLIENT --query='drop table if exists test_inserts'
$CLICKHOUSE_CLIENT --query='create table test_inserts engine=Null AS system.numbers'

for ((i = 1; i <= $total_iterations; i++)); do
    while true; do
        running_jobs=$(jobs -p | wc -w)
        if [ $running_jobs -lt $iterations_in_parallel ]; then
            break
        fi
        sleep 0.01
    done
    (
        (
            $CLICKHOUSE_LOCAL --query='SELECT * FROM numbers(1000000)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test_inserts%20FORMAT%20TSV' --data-binary @- 2>/dev/null
            $CLICKHOUSE_CLIENT -q "insert into test_inserts select * from numbers_mt(1e6)"
            # $CLICKHOUSE_CLIENT --query="WITH (SELECT memory_usage FROM system.user_processes WHERE user='default') as user_mem, (SELECT value FROM system.metrics WHERE metric='MemoryTracking') AS system_mem SELECT throwIf(user_mem > system_mem) FORMAT Null"
    ) &)
done

wait
