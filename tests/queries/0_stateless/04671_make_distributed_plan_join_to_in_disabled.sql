-- Tags: no-old-analyzer
-- `query_plan_convert_join_to_in` is not applied under `make_distributed_plan`: the set it
-- would create uses `transform_null_in = false` and transfer limits, which the serialized set
-- record does not carry, so a worker would rebuild the set with a different policy.

CREATE TABLE tj1 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tj2 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO tj1 SELECT number, number FROM numbers(100);
INSERT INTO tj2 SELECT number * 2, number FROM numbers(50);

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1, join_algorithm = 'hash', query_plan_convert_join_to_in = 1;
SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- The conversion declines while a join or transfer limit is active, and the test profile sets all four.
SET max_rows_in_join = 0, max_bytes_in_join = 0, max_rows_to_transfer = 0, max_bytes_to_transfer = 0;

SELECT '-- without make_distributed_plan the join converts to IN';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tj1 SEMI LEFT JOIN tj2 ON tj1.id = tj2.id)
    WHERE explain ILIKE '%CreatingSet%' OR trimLeft(explain) LIKE 'Join%';

SELECT '-- with make_distributed_plan the join is kept';
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT trimLeft(explain) FROM (EXPLAIN distributed = 1 SELECT count() FROM tj1 SEMI LEFT JOIN tj2 ON tj1.id = tj2.id)
    WHERE explain ILIKE '%CreatingSet%' OR trimLeft(explain) LIKE 'Join%';

SELECT '-- results agree';
SELECT count() FROM tj1 SEMI LEFT JOIN tj2 ON tj1.id = tj2.id;
SELECT count() FROM tj1 SEMI LEFT JOIN tj2 ON tj1.id = tj2.id SETTINGS make_distributed_plan = 0;

DROP TABLE tj1;
DROP TABLE tj2;
