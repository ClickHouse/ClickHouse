SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

CREATE TABLE t(number UInt64) ENGINE=MergeTree ORDER BY () AS SELECT * FROM numbers_mt(1e6);

SELECT AVG(transfer) FROM (SELECT number, SUM(number) AS transfer FROM t GROUP BY number) FORMAT Null;

CREATE TABLE crd
(
    polygon Array(Tuple(Float64, Float64))
)
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT [(0, 0), (0, 42), (42, 42), (42, 0)];

SELECT count() FROM t WHERE pointInPolygon((number, number), (SELECT * from crd)) FORMAT Null;

SELECT * FROM t UNION ALL SELECT * FROM t FORMAT Null;

SELECT * FROM t lhs INNER JOIN t rhs ON lhs.number = rhs.number LIMIT 1 FORMAT Null;

CREATE TABLE tt
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a
AS SELECT
    number,
    number * 2
FROM numbers_mt(1e5);

SELECT min(a) FROM tt SETTINGS optimize_aggregation_in_order=0;

