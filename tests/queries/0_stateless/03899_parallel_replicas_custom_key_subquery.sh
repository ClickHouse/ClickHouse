#!/usr/bin/env bash
# Tags: no-parallel, long
# no-parallel: reads its own rows from system.query_log by initial_query_id.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A query whose FROM contains a subquery over the target table used to fail under
# custom-key parallel replicas with NOT_FOUND_COLUMN_IN_BLOCK: the initiator numbers
# the nested table `__tableK` (K > 1), but a replica re-analyzes the query it receives
# from scratch and restarts the `__tableN` numbering at 1, so its result columns
# `__table1.*` did not match the initiator header. See issue #110678.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 03899_ck"
$CLICKHOUSE_CLIENT -q "CREATE TABLE 03899_ck (id UInt64, k UInt8, v UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO 03899_ck SELECT number, number % 7, number * 3 FROM numbers(1000)"

settings() {
    echo "SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3\
, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'\
, parallel_replicas_mode = '$1'\
, parallel_replicas_custom_key = 'id'\
, parallel_replicas_for_non_replicated_merge_tree = 1\
, prefer_localhost_replica = 0\
, serialize_query_plan = 0"
}

for mode in 'custom_key_sampling' 'custom_key_range'; do
    echo "mode=$mode"
    # Result must equal the non-parallel result for every subquery shape.
    $CLICKHOUSE_CLIENT -q "SELECT * FROM (SELECT id, k, v FROM 03899_ck WHERE id < 20) ORDER BY k, id $(settings "$mode")"
    $CLICKHOUSE_CLIENT -q "SELECT k, count(), sum(v) FROM (SELECT id, k, v FROM 03899_ck WHERE id < 500) GROUP BY k ORDER BY k $(settings "$mode")"
    $CLICKHOUSE_CLIENT -q "SELECT k, count(), sum(id) FROM (SELECT k, id FROM 03899_ck WHERE id % 2 = 0 UNION ALL SELECT k, id FROM 03899_ck WHERE id % 2 = 1) GROUP BY k ORDER BY k $(settings "$mode")"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT * FROM (SELECT id, k, v FROM 03899_ck WHERE id < 20)) $(settings "$mode")"

    # Engagement guard (the footgun in #110678): a comparison test passes vacuously if
    # custom-key mode silently does not engage. Assert per mode that the read actually ran
    # on remote replicas by counting the non-initial (remote) sub-queries in query_log.
    # Fresh per-invocation query_id (initial_query_id propagates to the remote sub-queries)
    # plus a tight event_time bound isolate the guard from earlier runs in the same database
    # (--test-runs, CI retries, same-day reruns cannot satisfy the count). See #110690.
    query_id="03899_ck_engage_${mode}_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
    start_time=$($CLICKHOUSE_CLIENT -q "SELECT now()")
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "SELECT * FROM (SELECT id, k, v FROM 03899_ck WHERE id < 20) ORDER BY k, id $(settings "$mode") FORMAT Null"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    echo -n "engaged remote replicas > 1: "
    $CLICKHOUSE_CLIENT -q "
        SELECT countDistinct(query) > 1
        FROM system.query_log
        WHERE has(databases, currentDatabase()) AND initial_query_id = '$query_id'
          AND is_initial_query = 0 AND type = 'QueryFinish'
          AND event_date >= yesterday() AND event_time >= '$start_time'"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE 03899_ck"
