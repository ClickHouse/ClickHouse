#!/usr/bin/env bash

# INSERT INTO Distributed SELECT FROM Distributed goes through
# StorageDistributed::distributedWriteBetweenDistributedTables, which assembles a
# completed pipeline with one remote pipeline per destination shard and runs it through
# CompletedPipelineExecutor. The thread count must be set explicitly, otherwise the
# executor runs single-threaded and the shards are written sequentially.
#
# A five-shard cluster (all on the same host) gives five independent remote pipelines.
# async_socket_for_remote=0 makes each remote pipeline occupy its own thread, so with the
# thread count set the shards overlap; sleepEachRow on the remote SELECT keeps them alive
# at the same time. peak_threads_usage also counts the executor's driver/polling thread,
# so the single-threaded case already reports ~2; five shards give a margin well above
# that. prefer_localhost_replica=0 forces the remote pipeline path for the local shards;
# distributed_foreground_insert=1 makes the rows visible synchronously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS local_dist_threads_src;
DROP TABLE IF EXISTS local_dist_threads_dst;
DROP TABLE IF EXISTS dist_threads_src;
DROP TABLE IF EXISTS dist_threads_dst;

CREATE TABLE local_dist_threads_src (x UInt64) ENGINE = Memory;
CREATE TABLE local_dist_threads_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_threads_src AS local_dist_threads_src ENGINE = Distributed('test_cluster_five_shards_localhost', currentDatabase(), local_dist_threads_src, x);
CREATE TABLE dist_threads_dst AS local_dist_threads_dst ENGINE = Distributed('test_cluster_five_shards_localhost', currentDatabase(), local_dist_threads_dst, x);

INSERT INTO local_dist_threads_src SELECT number FROM numbers(2);
"

run_mode()
{
    local mode=$1
    local query_id="04402_dist_threads_${mode}_$CLICKHOUSE_DATABASE"

    $CLICKHOUSE_CLIENT --query_id="$query_id" \
        --prefer_localhost_replica=0 \
        --parallel_distributed_insert_select="$mode" \
        --distributed_foreground_insert=1 \
        --async_socket_for_remote=0 \
        --max_threads=8 \
        --use_concurrency_control=0 \
        --send_logs_level=fatal \
        -q "INSERT INTO dist_threads_dst SELECT x FROM dist_threads_src WHERE NOT sleepEachRow(0.1)"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT peak_threads_usage > 3
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
          AND is_initial_query AND query_id = '$query_id'
    "
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_dist_threads_dst"
}

run_mode 1
run_mode 2

$CLICKHOUSE_CLIENT -q "
DROP TABLE local_dist_threads_src;
DROP TABLE local_dist_threads_dst;
DROP TABLE dist_threads_src;
DROP TABLE dist_threads_dst;
"
