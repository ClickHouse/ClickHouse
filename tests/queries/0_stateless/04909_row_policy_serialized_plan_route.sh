#!/usr/bin/env bash
# Tags: long, distributed, no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Counts, for the secondary queries of one initial query, how many ran a plan the initiator had
# already built. A node logs this message only where it built a runnable plan out of a received one.
# Prints "<secondary queries> <of them given a plan>".
route_of() {
    $CLICKHOUSE_CLIENT -q "
        SELECT count(), countIf(has_plan) FROM
        (
            SELECT query_id IN (
                SELECT query_id FROM system.text_log
                WHERE event_date >= yesterday() AND logger_name = 'TCPHandler'
                  AND message = 'Received query plan'
            ) AS has_plan
            FROM system.query_log
            WHERE type = 'QueryFinish' AND NOT is_initial_query
              AND event_date >= yesterday() AND initial_query_id = '$1'
              -- A secondary query logs current_database = 'default', not the test database.
              AND current_database IN ['default', currentDatabase()]
        )
        SETTINGS max_rows_to_read = 0"
}

# Secondary queries publish their log rows after the initiator has already returned, so a single
# flush can miss them; retry until the expected number of them is visible.
wait_for_route() {
    local result=""
    for _ in $(seq 1 60); do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, text_log"
        result=$(route_of "$1")
        [ "$(echo "$result" | cut -f1)" -ge "$2" ] && break
        sleep 0.5
    done
    echo "$result"
}

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rt_leaf;
    DROP TABLE IF EXISTS rt_dist;
    CREATE TABLE rt_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO rt_leaf SELECT number, number FROM numbers(10);
    CREATE TABLE rt_dist AS rt_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rt_leaf);
    CREATE ROW POLICY rt_leaf_policy ON rt_leaf FOR SELECT USING y < 5 TO ALL;"

# A row policy on the shard-local table yields the same rows either way, so only the log row below
# distinguishes a plan-shipped read from a text-shipped one.
for sqp in 1 0; do
    query_id="04909_dist_sqp${sqp}_$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "
        SELECT count() FROM rt_dist
        SETTINGS serialize_query_plan = $sqp, prefer_localhost_replica = 0,
                 optimize_trivial_count_query = 0, enable_analyzer = 1" > /dev/null
    # Printing the secondary-query count alongside makes a zero from an invisible row distinguishable
    # from a zero from a text-shipped read.
    echo -e "dist sqp=$sqp\t$(wait_for_route "$query_id" 1)"
done

$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY rt_leaf_policy ON rt_leaf;
    DROP TABLE rt_dist;
    DROP TABLE rt_leaf;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rt_pr;
    CREATE TABLE rt_pr (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0;
    INSERT INTO rt_pr SELECT number, number FROM numbers(2000000);
    CREATE ROW POLICY rt_pr_policy ON rt_pr FOR SELECT USING y < 1000000 TO ALL;"

query_id="04909_pr_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
    SELECT max(y) FROM rt_pr
    SETTINGS enable_analyzer = 1, serialize_query_plan = 1,
             enable_parallel_replicas = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0,
             parallel_replicas_mark_segment_size = 1, merge_tree_min_rows_for_concurrent_read = 1" > /dev/null
# Two remote replicas read; the third is the initiator's own local plan.
echo -e "parallel replicas\t$(wait_for_route "$query_id" 2 | cut -f2)"

$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY rt_pr_policy ON rt_pr;
    DROP TABLE rt_pr;"
