-- Regression test: a distributed query with `serialize_query_plan = 1` over hedged connections
-- must fall back to sending SQL when the plan carries non-default execution limits and a later
-- hedge could select a replica whose query-plan serialization version is unverified. Plan-level
-- `max_threads` and `concurrency_control` are serialized only since plan serialization version 10;
-- shipping the plan to an unverified peer would silently drop them during a rolling upgrade.

DROP TABLE IF EXISTS t_plan_limits_hedged_fallback;

CREATE TABLE t_plan_limits_hedged_fallback (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_plan_limits_hedged_fallback SELECT number FROM numbers(1000);

-- `serialize_query_plan` requires the analyzer; pin it against `compatibility` randomization.
-- The ordinary distributed path is under test; parallel replicas gate plan shipping differently.
SET enable_parallel_replicas = 0;
SET serialize_query_plan = 1;
-- Non-default plan-level execution limits, pinned against CI randomization: these are what the
-- fallback protects during a rolling upgrade.
SET max_threads = 4;
SET use_concurrency_control = 1;
-- Force a real remote connection: with the default the initiator would execute the only shard
-- locally and never open a connection, hedged or not.
SET prefer_localhost_replica = 0;

-- Hedged connections: the first replica is established up front, but a later hedge may select any
-- remaining replica of the pool, whose version is unknown at send time. With three replicas in the
-- pool only one is verified, so the initiator must choose the SQL fallback.
SELECT sum(a), count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_plan_limits_hedged_fallback)
SETTINGS use_hedged_requests = 1, log_comment = '05038_hedged_plan_limits_fallback';

-- Control: with hedged connections off every connected replica is verified (same-version local
-- cluster), so the plan is shipped and the fallback must stay silent.
SELECT sum(a), count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_plan_limits_hedged_fallback)
SETTINGS use_hedged_requests = 0, log_comment = '05038_hedged_plan_limits_no_fallback';

SYSTEM FLUSH LOGS query_log;

-- Both oracles read the replica legs' own `query_log` rows: a replica that receives SQL counts `SelectQuery`,
-- a replica that executes a shipped query plan creates no interpreter and does not.
-- The newest initial query is resolved by `log_comment` (CI reuses one database across executions in some
-- jobs, so an aggregate over history could be satisfied by an earlier run); a missing row prints nothing.

-- The firing oracle: every replica of the hedged query received SQL.
SELECT all_sql
FROM
(
    SELECT count() AS legs, min(ProfileEvents['SelectQuery']) > 0 AS all_sql
    FROM system.query_log
    WHERE initial_query_id = (
            SELECT query_id
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND log_comment = '05038_hedged_plan_limits_fallback'
              AND type = 'QueryFinish'
              AND is_initial_query
              AND event_date >= yesterday() AND event_time >= now() - 600
            ORDER BY event_time_microseconds DESC
            LIMIT 1)
      AND NOT is_initial_query
      AND type = 'QueryFinish'
      AND event_date >= yesterday() AND event_time >= now() - 600
)
WHERE legs > 0;

-- The control oracle: the replica of the non-hedged query executed the shipped plan.
SELECT max_select_queries
FROM
(
    SELECT count() AS legs, max(ProfileEvents['SelectQuery']) AS max_select_queries
    FROM system.query_log
    WHERE initial_query_id = (
            SELECT query_id
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND log_comment = '05038_hedged_plan_limits_no_fallback'
              AND type = 'QueryFinish'
              AND is_initial_query
              AND event_date >= yesterday() AND event_time >= now() - 600
            ORDER BY event_time_microseconds DESC
            LIMIT 1)
      AND NOT is_initial_query
      AND type = 'QueryFinish'
      AND event_date >= yesterday() AND event_time >= now() - 600
)
WHERE legs > 0;

DROP TABLE t_plan_limits_hedged_fallback;
