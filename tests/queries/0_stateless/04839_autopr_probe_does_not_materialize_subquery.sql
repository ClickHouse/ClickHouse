-- The plan the automatic-parallel-replicas decision builds to cost is usually thrown away, so it must
-- not execute the query's subqueries. Shipping a `GLOBAL JOIN` materializes its right side into a
-- temporary table while the plan is built, and those rows are discarded along with the plan.

DROP TABLE IF EXISTS t_04839_left;
DROP TABLE IF EXISTS t_04839_right;

CREATE TABLE t_04839_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_04839_right (k UInt64, s UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_04839_left SELECT number, number FROM numbers(1000000);
-- Big enough that materializing it is a large share of what the query reads, so the check below
-- cannot pass by accident.
INSERT INTO t_04839_right SELECT number, number FROM numbers(500000);

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1;

-- The decision must not change the answer.
SELECT count(), sum(l.k) FROM t_04839_left AS l GLOBAL JOIN t_04839_right AS r ON l.k = r.k
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT count(), sum(l.k) FROM t_04839_left AS l GLOBAL JOIN t_04839_right AS r ON l.k = r.k
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_min_bytes_per_replica = 0;

-- ... and must not read more than the single-node plan does. These read columns rather than wrap the
-- join in an aggregate, because the probe skips the plan entirely for a bare `count()`, which would
-- make the check below pass without ever exercising the path it is about.
SELECT l.k, l.v FROM t_04839_left AS l GLOBAL JOIN t_04839_right AS r ON l.k = r.k FORMAT Null
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, log_comment = '04839_single';

SELECT l.k, l.v FROM t_04839_left AS l GLOBAL JOIN t_04839_right AS r ON l.k = r.k FORMAT Null
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_min_bytes_per_replica = 0, log_comment = '04839_auto';

SYSTEM FLUSH LOGS query_log;

SELECT
    if(marks_auto <= marks_single, 'no extra marks', format('read {} marks against {} on a single node', marks_auto, marks_single))
FROM
(
    SELECT
        maxIf(ProfileEvents['SelectedMarks'], log_comment = '04839_auto') AS marks_auto,
        maxIf(ProfileEvents['SelectedMarks'], log_comment = '04839_single') AS marks_single
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
      AND event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE
      AND log_comment IN ('04839_single', '04839_auto')
);

DROP TABLE t_04839_left;
DROP TABLE t_04839_right;
