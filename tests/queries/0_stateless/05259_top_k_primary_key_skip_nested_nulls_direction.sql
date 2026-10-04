-- The top-K granule skipping through the primary index compares granule bounds with the threshold as raw
-- `Field` values, which place a NULL or NaN regardless of NULLS FIRST/LAST. The primary key stores such
-- values last, so under `NULLS FIRST` the granule holding them starts beyond the threshold and must not be
-- skipped. Top-level `Nullable` and floating-point columns are left out of the pruning; the same must hold
-- when the NULL or NaN is nested inside a `Tuple`.

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS t_top_k_nested_nullable;

CREATE TABLE t_top_k_nested_nullable (t Tuple(Nullable(UInt32))) ENGINE = MergeTree
ORDER BY t SETTINGS allow_nullable_key = 1, index_granularity = 8192;

INSERT INTO t_top_k_nested_nullable SELECT tuple(number) FROM numbers(1e5) UNION ALL SELECT tuple(NULL) FROM numbers(3);

SELECT groupArray(t) FROM (SELECT t FROM t_top_k_nested_nullable ORDER BY t ASC NULLS FIRST LIMIT 5);
SELECT groupArray(t) FROM (SELECT t FROM t_top_k_nested_nullable ORDER BY t ASC NULLS LAST LIMIT 2);

DROP TABLE t_top_k_nested_nullable;

DROP TABLE IF EXISTS t_top_k_nested_float;

CREATE TABLE t_top_k_nested_float (t Tuple(Float64)) ENGINE = MergeTree
ORDER BY t SETTINGS index_granularity = 8192;

INSERT INTO t_top_k_nested_float SELECT tuple(number) FROM numbers(1e5) UNION ALL SELECT tuple(nan) FROM numbers(3);

SELECT groupArray(t) FROM (SELECT t FROM t_top_k_nested_float ORDER BY t ASC NULLS FIRST LIMIT 5);
SELECT groupArray(t) FROM (SELECT t FROM t_top_k_nested_float ORDER BY t ASC NULLS LAST LIMIT 2);

DROP TABLE t_top_k_nested_float;
