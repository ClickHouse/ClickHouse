SET alter_sync = 2;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = true;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1__fuzz_26;

CREATE TABLE t1__fuzz_26 (`a` Nullable(Float64), `b` Nullable(Float32), `pk` Int64) ENGINE = MergeTree ORDER BY pk;
CREATE TABLE t1 ( a Float64, b Int64, pk String) Engine = MergeTree() ORDER BY pk;

ALTER TABLE t1
    (MODIFY COLUMN `a` Float64 TTL toDateTime(b) + toIntervalMonth(viewExplain('EXPLAIN', 'actions = 1', (
        SELECT
            toIntervalMonth(1),
            2
        FROM t1__fuzz_26
        GROUP BY
            toFixedString('%Prewhere%', 10),
            toNullable(12)
            WITH ROLLUP
    )), 1)) settings allow_experimental_parallel_reading_from_replicas = 1; -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

ALTER TABLE t1
    (MODIFY COLUMN `a` Float64 TTL toDateTime(b) + toIntervalMonth(viewExplain('EXPLAIN', 'actions = 1', (
        SELECT
            toIntervalMonth(1),
            2
        FROM t1__fuzz_26
        GROUP BY
            toFixedString('%Prewhere%', 10),
            toNullable(12)
            WITH ROLLUP
    )), 1)) settings allow_experimental_parallel_reading_from_replicas = 0; -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

DROP TABLE t1;
DROP TABLE t1__fuzz_26;
