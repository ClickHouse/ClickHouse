-- Regression test: SortingStep serialization must handle non-Full sorting types.
-- With the old interpreter (allow_experimental_analyzer=0), read-in-order optimization
-- creates a FinishSorting step during plan building (not during optimization).
-- When this plan is serialized for Distributed table queries, it throws NOT_IMPLEMENTED.

SET allow_experimental_analyzer = 0;
SET serialize_query_plan = 1;

DROP TABLE IF EXISTS t_sorting_local;
DROP TABLE IF EXISTS t_sorting_dist;

CREATE TABLE t_sorting_local (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_sorting_dist AS t_sorting_local ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_sorting_local);

INSERT INTO t_sorting_local SELECT number, toString(number) FROM numbers(1000);

-- ORDER BY id matches the primary key, so the old interpreter creates a FinishSorting step
-- directly during plan building. Serialization for the Distributed query must not throw.
SELECT id, value FROM t_sorting_dist ORDER BY id LIMIT 5;

DROP TABLE t_sorting_dist;
DROP TABLE t_sorting_local;
