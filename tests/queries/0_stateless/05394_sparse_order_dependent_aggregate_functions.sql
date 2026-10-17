-- Order-dependent aggregate functions must add the rows of a sparse argument in their order,
-- also without GROUP BY and when all rows of a block go to one key.
-- https://github.com/ClickHouse/ClickHouse/issues/123817

SET optimize_read_in_order = 1;
SET max_threads = 1;
SET allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE IF EXISTS t_sparse_order;

CREATE TABLE t_sparse_order (k UInt64, i UInt64, x UInt64, n Nullable(UInt64))
ENGINE = MergeTree ORDER BY i
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, nullable_serialization_version = 'allow_sparse', serialization_info_version = 'with_types';

INSERT INTO t_sparse_order SELECT 7, number, if(number % 5 IN (1, 2), number, 0), if(number % 5 IN (1, 2), number, NULL) FROM numbers(12);

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_order' AND column IN ('x', 'n') ORDER BY column;

-- The read in order keeps `x` and `n` sparse until the aggregation.
SELECT any(x), anyLast(x), groupArray(x), groupConcat(x), groupArrayLast(3)(x), deltaSum(x), groupArrayMovingSum(2)(x)
FROM (SELECT x FROM t_sparse_order ORDER BY i);

SELECT finalizeAggregation(groupArrayState(x)) FROM (SELECT x FROM t_sparse_order ORDER BY i);

SELECT any(n), anyLast(n), groupArray(n), any(n) RESPECT NULLS, anyLast(n) RESPECT NULLS
FROM (SELECT n FROM t_sparse_order ORDER BY i);

-- All rows of the block have the same key, so they are added to one place.
SELECT k, any(x), anyLast(x), groupArray(x) FROM (SELECT k, x FROM t_sparse_order ORDER BY i) GROUP BY k;

-- Order-independent functions keep adding the non-default values first.
SELECT sum(x), count(x), uniqExact(x), min(x), max(x), sum(n), count(n) FROM (SELECT x, n FROM t_sparse_order ORDER BY i);

DROP TABLE t_sparse_order;

-- The `GROUP BY` key is a prefix of the sorting key. The aggregation in order adds the rows of a key in one
-- go, after `AggregatingInOrderTransform` has made the sparse argument full.
CREATE TABLE t_sparse_order_in_order (k UInt64, i UInt64, x UInt64)
ENGINE = MergeTree ORDER BY (k, i)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_types';

INSERT INTO t_sparse_order_in_order SELECT intDiv(number, 6), number, if(number % 5 IN (1, 2), number, 0) FROM numbers(12);

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_order_in_order' AND column = 'x';

SELECT k, any(x), anyLast(x), groupArray(x), deltaSum(x) FROM t_sparse_order_in_order GROUP BY k ORDER BY k
SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE t_sparse_order_in_order;
