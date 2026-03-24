#!/usr/bin/env bash
# Tags: no-replicated-database
# - no-replicated-database - it uses cluster of multiple nodes, while we use different clusters

# Verify that distributed index analysis with parallel replicas does not produce
# a quadratic number of queries when the predicate contains an IN subquery.
# The `distributed_index_analysis_only_on_coordinator` setting restricts distributed
# index analysis to the coordinator and disables `enable_parallel_replicas` for the
# remote index analysis queries, preventing O(N^2) recursive spawning.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_in_dia;

    CREATE TABLE test_in_dia (key Int, value Int)
    ENGINE = MergeTree()
    ORDER BY key
    SETTINGS
        distributed_index_analysis_min_parts_to_activate = 0,
        distributed_index_analysis_min_indexes_bytes_to_activate = 0;

    SYSTEM STOP MERGES test_in_dia;

    INSERT INTO test_in_dia
    SELECT number, number * 100
    FROM numbers(100000)
    SETTINGS
        max_block_size = 10000,
        min_insert_block_size_rows = 10000,
        max_insert_threads = 1;
"

query_id="$RANDOM-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --query_id $query_id -q "
    SET automatic_parallel_replicas_mode = 0;
    SET parallel_replicas_for_non_replicated_merge_tree = 1;
    SET parallel_replicas_index_analysis_only_on_coordinator = 1;
    SET parallel_replicas_local_plan = 1;
    SET use_query_condition_cache = 0;
    SET distributed_index_analysis_for_non_shared_merge_tree = 1;
    SET enable_parallel_replicas = 1;
    SET distributed_index_analysis = 1;
    SET distributed_index_analysis_only_on_coordinator = 1;
    SET cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
    SET send_logs_level = 'error';

    SELECT sum(key)
    FROM test_in_dia
    WHERE key IN (SELECT key FROM test_in_dia WHERE key > 50000)
"

# Verify the total number of spawned queries is bounded (not quadratic).
# With dynamic work distribution the local plan (zero network latency) may consume
# all parts before the remote replica connects, causing the coordinator to cancel
# the unused RemoteSource. In that case the worker data-reading query is never sent
# and no query_log entry is created, so we only assert a bounded count and show
# the deterministic queries (initial + index analysis).
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;

    -- Total query count must be bounded, not quadratic.
    SELECT if (count() BETWEEN 3 AND 4, 'OK', format('Expected 3-4 queries, got {}', count())) AS result
    FROM system.query_log
    WHERE
        event_date >= yesterday() AND event_time >= now() - 600
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND initial_query_id = '$query_id'
        AND (current_database = currentDatabase() OR 1);

    -- Deterministic queries: the initial query and index analysis queries.
    SELECT
        count(),
        sum(ProfileEvents['DistributedIndexAnalysisScheduledReplicas']) > 0,
        replaceOne(trimRight(normalizeQuery(any(query))), '\`?\`.', 'default.') AS normalized_query
    FROM system.query_log
    WHERE
        event_date >= yesterday() AND event_time >= now() - 600
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND initial_query_id = '$query_id'
        AND (current_database = currentDatabase() OR 1)
        AND (is_initial_query OR query LIKE '%mergeTreeAnalyzeIndexes%')
    GROUP BY
        normalized_query_hash
    ORDER BY normalized_query;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_in_dia"
