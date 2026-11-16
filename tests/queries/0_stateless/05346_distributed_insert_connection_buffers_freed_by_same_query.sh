#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A synchronous INSERT into a Distributed table sends its data over a pooled connection. Every
# large allocation the INSERT makes for that must be freed by the same INSERT, not by the next
# query that takes the connection from the pool.
# The data is sent with `LZ4`: the contexts of the `ZSTD` codec are pooled and shared by all queries on purpose,
# so a query may free the workspace of a context that another query allocated.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE local (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);
"

for i in $(seq 1 8); do
    $CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}" -q "
        INSERT INTO dist SELECT number, repeat('x', 1000) FROM numbers(10000)
        SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0, network_compression_method = 'LZ4',
            max_untracked_memory = 0, memory_profiler_sample_probability = 1,
            memory_profiler_sample_min_allocation_size = 500000"
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS trace_log"

# Frees, in each INSERT, of memory that the INSERT did not allocate itself. Only samples charged to the query count:
# a sample taken under a memory tracker blocker, such as a shared system log queue regrowing, is not charged to it.
$CLICKHOUSE_CLIENT -q "
    SELECT uniqExact(query_id), countIf(first_size < 0)
    FROM
    (
        SELECT query_id, argMin(size, event_time_microseconds) AS first_size
        FROM system.trace_log
        WHERE event_date >= yesterday() AND trace_type = 'MemorySample' AND memory_blocked_context = 'Max'
            AND query_id LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\\_%'
        GROUP BY query_id, ptr
    )
"
