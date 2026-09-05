-- Positive runtime filters reject only an outer Nullable NULL. NULLs nested in a Tuple,
-- Dynamic, or Variant remain hashable join-key values and must match as they do without the filter.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 1;
SET allow_dynamic_type_in_join_keys = 1;
SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS rf_null_shape_l_05054;
DROP TABLE IF EXISTS rf_null_shape_r_05054;

-- Outer Nullable: NULL does not match. Exercise both exact and Bloom runtime filters.
CREATE TABLE rf_null_shape_l_05054 (k Nullable(Int32)) ENGINE = Memory;
CREATE TABLE rf_null_shape_r_05054 (k Nullable(Int32)) ENGINE = Memory;
INSERT INTO rf_null_shape_l_05054 VALUES (NULL), (1), (2), (3);
INSERT INTO rf_null_shape_r_05054 VALUES (NULL), (1), (2);

SELECT
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 100),
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 1);

DROP TABLE rf_null_shape_l_05054;
DROP TABLE rf_null_shape_r_05054;

-- Tuple(Nullable): the nested NULL is part of the hashable Tuple key.
CREATE TABLE rf_null_shape_l_05054 (k Tuple(Nullable(Int32))) ENGINE = Memory;
CREATE TABLE rf_null_shape_r_05054 (k Tuple(Nullable(Int32))) ENGINE = Memory;
INSERT INTO rf_null_shape_l_05054 VALUES (tuple(NULL)), (tuple(1)), (tuple(2)), (tuple(3));
INSERT INTO rf_null_shape_r_05054 VALUES (tuple(NULL)), (tuple(1)), (tuple(2));

SELECT
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 100);

DROP TABLE rf_null_shape_l_05054;
DROP TABLE rf_null_shape_r_05054;

-- Nullable(Tuple(Nullable)): skip the outer NULL but preserve the nested NULL.
CREATE TABLE rf_null_shape_l_05054 (k Nullable(Tuple(Nullable(Int32), Int32))) ENGINE = Memory;
CREATE TABLE rf_null_shape_r_05054 (k Nullable(Tuple(Nullable(Int32), Int32))) ENGINE = Memory;
INSERT INTO rf_null_shape_l_05054 VALUES
    (CAST(NULL AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(NULL, 10) AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(1, 10) AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(2, 10) AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(3, 10) AS Nullable(Tuple(Nullable(Int32), Int32))));
INSERT INTO rf_null_shape_r_05054 VALUES
    (CAST(NULL AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(NULL, 10) AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(1, 10) AS Nullable(Tuple(Nullable(Int32), Int32)))),
    (CAST(tuple(2, 10) AS Nullable(Tuple(Nullable(Int32), Int32))));

SELECT
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 100);

DROP TABLE rf_null_shape_l_05054;
DROP TABLE rf_null_shape_r_05054;

-- Dynamic and Variant represent NULL with an internal discriminator, not an outer Nullable map.
CREATE TABLE rf_null_shape_l_05054 (k Dynamic) ENGINE = Memory;
CREATE TABLE rf_null_shape_r_05054 (k Dynamic) ENGINE = Memory;
INSERT INTO rf_null_shape_l_05054 VALUES (NULL), (true), (false), ('str');
INSERT INTO rf_null_shape_r_05054 VALUES (NULL), (true), ('str');

SELECT
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 100);

DROP TABLE rf_null_shape_l_05054;
DROP TABLE rf_null_shape_r_05054;

CREATE TABLE rf_null_shape_l_05054 (k Variant(String, Bool)) ENGINE = Memory;
CREATE TABLE rf_null_shape_r_05054 (k Variant(String, Bool)) ENGINE = Memory;
INSERT INTO rf_null_shape_l_05054 VALUES (NULL), (true), (false), ('str');
INSERT INTO rf_null_shape_r_05054 VALUES (NULL), (true), ('str');

SELECT
    (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 0)
        = (SELECT count() FROM rf_null_shape_l_05054 INNER JOIN rf_null_shape_r_05054 USING (k) SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 100);

DROP TABLE rf_null_shape_l_05054;
DROP TABLE rf_null_shape_r_05054;
