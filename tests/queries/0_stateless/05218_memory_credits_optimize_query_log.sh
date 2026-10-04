#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A synchronous OPTIMIZE receives the profile events of the merge it runs only through the replay of the
# merge thread group's counters into the initial query. MemoryCredits (the time integral of memory usage)
# is charged to the merge group's own tracker, so it must be flushed into the merge group's counters before
# that replay: the OPTIMIZE row of system.query_log must carry the whole value that system.part_log records
# for the same merge (plus whatever the query itself held), and never less.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS memory_credits_optimize"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE memory_credits_optimize (n UInt64, s String) ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0"

for i in 0 1; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO memory_credits_optimize SELECT number + $i * 10000, repeat('x', 4096) FROM numbers(10000)"
done

${CLICKHOUSE_CLIENT} --log_queries 1 --log_comment '05218_memory_credits_optimize' --query "OPTIMIZE TABLE memory_credits_optimize FINAL"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log, part_log"

${CLICKHOUSE_CLIENT} --query "
    WITH
        (
            SELECT ProfileEvents['MemoryCredits']
            FROM system.part_log
            WHERE database = currentDatabase() AND table = 'memory_credits_optimize' AND event_type = 'MergeParts'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
        ) AS merge_credits,
        (
            SELECT ProfileEvents['MemoryCredits']
            FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '05218_memory_credits_optimize' AND type = 'QueryFinish'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
        ) AS query_credits
    SELECT merge_credits > 0, query_credits >= merge_credits"

${CLICKHOUSE_CLIENT} --query "DROP TABLE memory_credits_optimize"
