DROP TABLE IF EXISTS rf_skip_probe;
DROP TABLE IF EXISTS rf_skip_build;

CREATE TABLE rf_skip_probe (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE rf_skip_build (a UInt64 STATISTICS(uniq), b UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rf_skip_probe SELECT number, number % 2 FROM numbers(1000);
INSERT INTO rf_skip_build SELECT number, number % 2 FROM numbers(2000);
ALTER TABLE rf_skip_build MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_fixed_hash_table_conversion = 0;
SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'hash';
SET join_runtime_filter_exact_values_limit = 10;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_size_from_hash_table_stats = 0;
SET join_runtime_filter_use_minmax = 0;
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.01;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 1;
SET use_statistics = 1;

-- The only key is saturated, so no build or apply step is added and the join remains unchanged.
SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_skip_probe AS p
    INNER JOIN rf_skip_build AS q ON p.a = q.a
)
WHERE explain LIKE '%Build runtime join filter%';

SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_skip_probe AS p
    INNER JOIN rf_skip_build AS q ON p.a = q.a
)
WHERE explain LIKE '%Apply runtime join filter%';

SELECT count()
FROM rf_skip_probe AS p
INNER JOIN rf_skip_build AS q ON p.a = q.a;

-- Key `a` is skipped, while low-NDV key `b` remains on the exact membership path.
SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_skip_probe AS p
    INNER JOIN rf_skip_build AS q ON p.a = q.a AND p.b = q.b
)
WHERE explain LIKE '%Build runtime join filter%';

SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_skip_probe AS p
    INNER JOIN rf_skip_build AS q ON p.a = q.a AND p.b = q.b
)
WHERE explain LIKE '%Apply runtime join filter%';

SELECT count()
FROM rf_skip_probe AS p
INNER JOIN rf_skip_build AS q ON p.a = q.a AND p.b = q.b;

DROP TABLE rf_skip_probe;
DROP TABLE rf_skip_build;
