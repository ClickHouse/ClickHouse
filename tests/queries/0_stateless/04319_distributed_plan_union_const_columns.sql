-- Tags: no-old-analyzer

-- A distributed UNION can produce a column as a constant in one branch and as a full column
-- (aliased from an exchange) in the other. Plan serialization re-derives constness per step, so the
-- branches used to mismatch at the strict UnionStep header check. Constants are now materialized on
-- every branch so they agree.

DROP TABLE IF EXISTS t_union_const;
CREATE TABLE t_union_const (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_union_const SELECT number FROM numbers(1000000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1;

SELECT DISTINCT toFixedString(NULL, 'null'), minus(NULL, (SELECT NULL))
FROM t_union_const WHERE x < 1 GROUP BY 1, NULL
UNION DISTINCT
SELECT DISTINCT toFixedString(NULL, 'null'), minus(NULL, (SELECT NULL))
FROM t_union_const WHERE x < 9 GROUP BY 1, NULL;

DROP TABLE t_union_const;
