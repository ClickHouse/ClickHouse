-- `automatic_parallel_replicas_ignore_thresholds` is what the `AutoParallelReplicas` stateless jobs
-- rely on to reach the plan rewrite at all: without it a read too small to occupy `max_threads` is
-- decided by the reading-thread cap before the cost model is consulted, and those jobs go back to
-- exercising the probe rather than the switch. Nothing asserted the = 1 branch, so it could stop
-- working while the jobs stayed green - 04035 covers only the = 0 side.
--
-- The cap clamps BOTH sides of the comparison to the same thread count for a small read, which leaves
-- `input / n ? (input / n + output / replicas)` - true only if the output were free, so the query never
-- switches. Ignoring the cap divides by the real thread counts instead, and the same query switches.

DROP TABLE IF EXISTS t_autopr_cap;

CREATE TABLE t_autopr_cap (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- The gate this setting deliberately does NOT bypass. Pinned to 0 so that the only threshold left to
-- decide these queries is the reading-thread cap, which is what the test is about.
SET automatic_parallel_replicas_min_bytes_per_replica=0;

-- The cap is derived from this, and the branch also short-circuits when it is 0 - in which case both
-- arms below would behave identically and the test would assert nothing. The settings randomizer picks
-- 1Mi/8Mi/16Mi for its alias `filesystem_prefetch_min_bytes_for_single_read_task`, never 0, but pin it
-- anyway so the arms differ for a stated reason rather than by luck.
SET merge_tree_min_bytes_per_task_for_remote_reading='2Mi';

INSERT INTO t_autopr_cap SELECT number, number FROM numbers(20000);

-- Empty cache: this one only collects statistics, so it cannot switch whatever the thresholds say.
SELECT k % 997 AS g, count() FROM t_autopr_cap GROUP BY g ORDER BY g FORMAT Null
    SETTINGS log_comment='05259_query_0_collect', automatic_parallel_replicas_ignore_thresholds=0;

-- Statistics are available now. With the cap in force the two sides differ only by the output term, so
-- the comparison cannot favour replicas and the plan is left alone.
SELECT k % 997 AS g, count() FROM t_autopr_cap GROUP BY g ORDER BY g FORMAT Null
    SETTINGS log_comment='05259_query_1_cap_applies', automatic_parallel_replicas_ignore_thresholds=0;

-- Same query, same statistics, cap ignored: the read is divided by `max_threads` on one side and by
-- `max_threads * max_parallel_replicas` on the other, and the switch happens.
SELECT k % 997 AS g, count() FROM t_autopr_cap GROUP BY g ORDER BY g FORMAT Null
    SETTINGS log_comment='05259_query_2_cap_ignored', automatic_parallel_replicas_ignore_thresholds=1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS switched_to_replicas
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
  AND (current_database = currentDatabase()) AND (log_comment LIKE '05259_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_cap;
