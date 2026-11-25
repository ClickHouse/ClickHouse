-- Tags: no-old-analyzer

-- `log_query_plans_time` and `log_query_plans_join_matches` turn on the collection that the `time`
-- and `matches` settings of `EXPLAIN ANALYZE` turn on, and the plan records under `Collected` which
-- of them the query ran with.

DROP TABLE IF EXISTS p_left;
DROP TABLE IF EXISTS p_right;

CREATE TABLE p_left (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE p_right (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO p_left SELECT number FROM numbers(20000);
INSERT INTO p_right SELECT number * 2 FROM numbers(5000);

SET log_query_plans = 1;

-- The right side of an `ALL INNER` hash join reports how many of its rows matched only with the
-- extra bookkeeping, because nothing the join produces anyway carries that number.
-- The plan is captured on the initiator only, and with parallel replicas its share of the join can be empty.
SELECT count() FROM p_left ALL INNER JOIN p_right USING (k)
SETTINGS log_comment = '05315_default', join_algorithm = 'hash', enable_parallel_replicas = 0 FORMAT Null;

SELECT count() FROM p_left ALL INNER JOIN p_right USING (k)
SETTINGS log_comment = '05315_both', join_algorithm = 'hash', log_query_plans_time = 1, log_query_plans_join_matches = 1, enable_parallel_replicas = 0 FORMAT Null;

SET log_query_plans = 0;

-- Without `log_query_plans` the two settings capture nothing on their own.
SELECT count() FROM p_left ALL INNER JOIN p_right USING (k)
SETTINGS log_comment = '05315_off', join_algorithm = 'hash', log_query_plans_time = 1, log_query_plans_join_matches = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    JSONExtractBool(plan, 'Collected', 'Time'),
    JSONExtractBool(plan, 'Collected', 'JoinMatches'),
    -- Whether any step reports the `Time` and `Concurrency` groups, which come from the work intervals.
    arrayExists(n -> JSONHas(n, 'Statistics', 'Time'), JSONExtractArrayRaw(plan, 'Nodes')),
    arrayExists(n -> JSONHas(n, 'Statistics', 'Concurrency'), JSONExtractArrayRaw(plan, 'Nodes')),
    -- The query-level split of the execution time comes from the work intervals too.
    JSONHas(plan, 'ExecutionTimeBreakdown'),
    JSONExtractUInt(plan, 'ExecutionTimeBreakdown', 'InStepsNs') > 0,
    -- Which table ends up on the build side is the optimiser's choice, so the count is not pinned.
    JSONExtractUInt(join_stats, 'Right', 'Matched') > 0
FROM
(
    SELECT
        log_comment,
        toJSONString(query_plan) AS plan,
        JSONExtractRaw(
            arrayFilter(n -> JSONExtractString(n, 'NodeType') = 'Join', JSONExtractArrayRaw(plan, 'Nodes'))[1],
            'Statistics') AS join_stats
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05315_default', '05315_both')
)
ORDER BY log_comment;

SELECT 'off', count(), anyLast(toJSONString(query_plan))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05315_off';

DROP TABLE p_left;
DROP TABLE p_right;
