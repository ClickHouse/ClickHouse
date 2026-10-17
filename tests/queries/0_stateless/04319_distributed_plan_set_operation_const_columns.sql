-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A distributed set operation (UNION / INTERSECT / EXCEPT) can produce a column as a constant in one
-- branch and as a full column (aliased from an exchange) in another. Plan serialization re-derives
-- constness per step, so the branches used to mismatch at the strict set-operation header check.
-- Constants are now materialized on every branch so they agree.

DROP TABLE IF EXISTS t_union_const;
CREATE TABLE t_union_const (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_union_const SELECT number FROM numbers(1000000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1;

-- UNION: a constant in one branch vs a full column aliased from an exchange in the other.
SELECT DISTINCT toFixedString(NULL, 'null'), minus(NULL, (SELECT NULL))
FROM t_union_const WHERE x < 1 GROUP BY 1, NULL
UNION DISTINCT
SELECT DISTINCT toFixedString(NULL, 'null'), minus(NULL, (SELECT NULL))
FROM t_union_const WHERE x < 9 GROUP BY 1, NULL;

DROP TABLE t_union_const;

-- INTERSECT / EXCEPT with a UNION of constants inside each input. The set operation must be the root
-- step to reproduce (a parent step would be distributed and reject the unserializable set operation),
-- so the result cannot be ordered; use FORMAT Null.
SET distributed_plan_max_rows_to_broadcast = 0;

WITH cte AS (SELECT DISTINCT -9223372036854775808 UNION ALL SELECT DISTINCT NULL LIMIT 1025)
SELECT DISTINCT toNullable(NULL), * FROM cte LIMIT 65536
INTERSECT DISTINCT
WITH cte AS (SELECT DISTINCT -9223372036854775808 UNION ALL SELECT DISTINCT NULL LIMIT 1025)
SELECT DISTINCT NULL, * FROM cte LIMIT 65536
FORMAT Null;

WITH cte AS (SELECT DISTINCT -9223372036854775808 UNION ALL SELECT DISTINCT NULL LIMIT 1025)
SELECT DISTINCT toNullable(NULL), * FROM cte LIMIT 65536
EXCEPT DISTINCT
WITH cte AS (SELECT DISTINCT 0 UNION ALL SELECT DISTINCT NULL LIMIT 1025)
SELECT DISTINCT NULL, * FROM cte LIMIT 65536
FORMAT Null;
