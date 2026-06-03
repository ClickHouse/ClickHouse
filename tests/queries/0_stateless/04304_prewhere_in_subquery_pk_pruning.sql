-- Tags: no-parallel-replicas
DROP TABLE IF EXISTS t_04304 SYNC;

CREATE TABLE t_04304
(
    client_id UInt32,
    request_time DateTime64(3, 'UTC'),
    payload String
)
ENGINE = MergeTree
ORDER BY (client_id, request_time)
SETTINGS index_granularity = 8192;

INSERT INTO t_04304
SELECT number % 100, now() - toIntervalSecond(number % 604800), 'x'
FROM numbers(200000);

SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET use_index_for_in_with_subqueries = 1;

-- The PrimaryKey condition for PREWHERE col IN (subquery) must reference the key set,
-- not collapse to "true" (issue #106336).
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM t_04304
    PREWHERE client_id IN (SELECT client_id FROM t_04304 LIMIT 10)
)
WHERE explain LIKE '%Condition:%';

DROP TABLE t_04304 SYNC;
