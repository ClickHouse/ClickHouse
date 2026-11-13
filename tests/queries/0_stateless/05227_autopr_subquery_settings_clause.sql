-- Automatic parallel replicas must behave the same when an `IN` subquery carries its own `SETTINGS`
-- clause. The optimization costs a probe plan built from a clone of the query AST with
-- `automatic_parallel_replicas_mode` and `force_primary_key` dropped from it, and matches that plan
-- against the single-node plan by tree hash. A subquery's `SETTINGS` clause is part of its query-node
-- tree hash, so rewriting it in the clone makes the two plans disagree about the same `IN` set: the set
-- transplant then fails with `Cannot find a matching set in the map of sets from single-replica plan`
-- when the set is consumed above the matched node, and the matched read step is not found at all, so
-- parallel replicas are silently skipped, when it is consumed at or below it.
--
-- Both names the optimization strips are exercised, each at the value already in force here: the default
-- `force_primary_key = 0`, and `automatic_parallel_replicas_mode = 1` as set for this file. A nested
-- clause therefore changes nothing but the subquery's hash.

DROP TABLE IF EXISTS t_autopr_subquery_settings;
DROP TABLE IF EXISTS t_autopr_subquery_settings_mid;

CREATE TABLE t_autopr_subquery_settings (key UInt64, non_key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 128;
CREATE TABLE t_autopr_subquery_settings_mid (x UInt64) ENGINE = MergeTree ORDER BY x;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- Both prewhere switches are pinned because the harness randomizes each of them off independently: the
-- `WHERE` shapes below are the shape under test only when the `WHERE` becomes a `PREWHERE`, which is
-- where the diverging set name reaches the read step's hash.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- May disable the usage of parallel replicas
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t_autopr_subquery_settings SELECT number, number % 500 FROM numbers(1e5);
INSERT INTO t_autopr_subquery_settings_mid SELECT number FROM numbers(300);

-- Everything but the clause under test is passed out of band, so the only `SETTINGS` clause inside a
-- query below is the nested one. In a pair, the first query primes the dataflow statistics and the second
-- can then use parallel replicas. The last pair is the exception in the reference: its matched node is the
-- first pair's aggregate read, which holds no set in its hash and is already primed, whereas a `WHERE`
-- pair's matched read step hashes the generated set name and so misses.

-- The set is consumed above the node the optimization matches, in an `ORDER BY` expression.
SELECT non_key, count() AS c FROM t_autopr_subquery_settings GROUP BY non_key
    ORDER BY c DESC, (non_key IN (SELECT number FROM numbers(500) SETTINGS force_primary_key = 0)) DESC LIMIT 10
    SETTINGS log_comment='05227_autopr_sq_a0' FORMAT Null;

SELECT non_key, count() AS c FROM t_autopr_subquery_settings GROUP BY non_key
    ORDER BY c DESC, (non_key IN (SELECT number FROM numbers(500) SETTINGS force_primary_key = 0)) DESC LIMIT 10
    SETTINGS log_comment='05227_autopr_sq_a1' FORMAT Null;

-- The set is consumed at the matched node, in a `WHERE` that becomes `PREWHERE`.
SELECT sum(key) FROM t_autopr_subquery_settings
WHERE non_key IN (SELECT x FROM t_autopr_subquery_settings_mid SETTINGS force_primary_key = 0)
    SETTINGS log_comment='05227_autopr_sq_b0' FORMAT TSV;

SELECT sum(key) FROM t_autopr_subquery_settings
WHERE non_key IN (SELECT x FROM t_autopr_subquery_settings_mid SETTINGS force_primary_key = 0)
    SETTINGS log_comment='05227_autopr_sq_b1' FORMAT TSV;

-- The other name the probe overrides, in both positions: `automatic_parallel_replicas_mode` is the one
-- `buildContext` reacts to, so it is not interchangeable with the clause above.
SELECT sum(key) FROM t_autopr_subquery_settings
WHERE non_key IN (SELECT x FROM t_autopr_subquery_settings_mid SETTINGS automatic_parallel_replicas_mode = 1)
    SETTINGS log_comment='05227_autopr_sq_c0' FORMAT TSV;

SELECT sum(key) FROM t_autopr_subquery_settings
WHERE non_key IN (SELECT x FROM t_autopr_subquery_settings_mid SETTINGS automatic_parallel_replicas_mode = 1)
    SETTINGS log_comment='05227_autopr_sq_c1' FORMAT TSV;

SELECT non_key, count() AS c FROM t_autopr_subquery_settings GROUP BY non_key
    ORDER BY c DESC, (non_key IN (SELECT number FROM numbers(500) SETTINGS automatic_parallel_replicas_mode = 1)) DESC LIMIT 10
    SETTINGS log_comment='05227_autopr_sq_d0' FORMAT Null;

SELECT non_key, count() AS c FROM t_autopr_subquery_settings GROUP BY non_key
    ORDER BY c DESC, (non_key IN (SELECT number FROM numbers(500) SETTINGS automatic_parallel_replicas_mode = 1)) DESC LIMIT 10
    SETTINGS log_comment='05227_autopr_sq_d1' FORMAT Null;

SET enable_parallel_replicas=0;

SYSTEM FLUSH LOGS query_log;

-- `pr_used` is what keeps the four pairs from passing on a run where parallel replicas were never chosen.
SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05227_autopr_sq_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_subquery_settings;
DROP TABLE t_autopr_subquery_settings_mid;
