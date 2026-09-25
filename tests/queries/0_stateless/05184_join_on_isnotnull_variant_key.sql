SET enable_analyzer = 1;
-- The retained guard is a one-sided ON condition, which the merge-family algorithms do not
-- implement; the stress test randomizes join_algorithm.
SET join_algorithm = 'hash';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_variant_left;
DROP TABLE IF EXISTS t_variant_right;
DROP TABLE IF EXISTS t_nullable_left;
DROP TABLE IF EXISTS t_nullable_right;
DROP TABLE IF EXISTS t_lc_left;
DROP TABLE IF EXISTS t_lc_right;
DROP TABLE IF EXISTS t_plain_left;
DROP TABLE IF EXISTS t_plain_right;
DROP TABLE IF EXISTS t_dynamic_left;
DROP TABLE IF EXISTS t_dynamic_right;

CREATE TABLE t_variant_left (k Variant(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_variant_right (k Variant(Int64), tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_left VALUES (10::Int64), (NULL);
INSERT INTO t_variant_right VALUES (10::Int64, 'ten'), (NULL, 'nul');

-- A Variant NULL is a discriminator value, so the join keys on it like any other value and the
-- IS NOT NULL conjuncts are the only thing excluding the NULL/NULL pair.
SELECT 'variant inner', count()
FROM t_variant_left AS l INNER JOIN t_variant_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL;

SELECT 'variant left', toString(l.k) AS lk, r.tag AS tag
FROM t_variant_left AS l LEFT JOIN t_variant_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
ORDER BY lk, tag;

SELECT 'variant guard kept', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_variant_left AS l INNER JOIN t_variant_right AS r
        ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
) WHERE explain ILIKE '%function_name: isNotNull%';

-- A true constant is redundant for every key type, so the collapse must still happen here; only the
-- isNotNull guard is type-dependent.
SELECT 'variant true collapsed', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_variant_left AS l INNER JOIN t_variant_right AS r
        ON l.k = r.k AND true
) WHERE explain LIKE '%CONSTANT%';

SELECT 'variant true merge join', count()
FROM t_variant_left AS l INNER JOIN t_variant_right AS r ON l.k = r.k AND true
SETTINGS join_algorithm = 'full_sorting_merge';

CREATE TABLE t_nullable_left (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_nullable_right (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_left VALUES (10), (NULL);
INSERT INTO t_nullable_right VALUES (10), (NULL);

CREATE TABLE t_lc_left (k LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lc_right (k LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_left VALUES ('a'), (NULL);
INSERT INTO t_lc_right VALUES ('a'), (NULL);

CREATE TABLE t_plain_left (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_plain_right (k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_plain_left VALUES (10), (20);
INSERT INTO t_plain_right VALUES (10), (30);

-- The rewrite must still fire for the key types whose NULL the join already excludes: a Nullable
-- NULL, also inside LowCardinality, is dropped by the null-key map, and IS NOT NULL over a
-- non-nullable key folds to a constant.
SELECT 'nullable inner', count()
FROM t_nullable_left AS l INNER JOIN t_nullable_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL;

SELECT 'nullable guard dropped', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_nullable_left AS l INNER JOIN t_nullable_right AS r
        ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
) WHERE explain ILIKE '%function_name: isNotNull%';

SELECT 'lc nullable inner', count()
FROM t_lc_left AS l INNER JOIN t_lc_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL;

SELECT 'lc nullable guard dropped', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_lc_left AS l INNER JOIN t_lc_right AS r
        ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
) WHERE explain ILIKE '%function_name: isNotNull%';

SELECT 'plain inner', count()
FROM t_plain_left AS l INNER JOIN t_plain_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL;

SELECT 'plain guard dropped', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_plain_left AS l INNER JOIN t_plain_right AS r
        ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
) WHERE explain ILIKE '%function_name: isNotNull%' OR explain LIKE '%CONSTANT%';

SET allow_dynamic_type_in_join_keys = 1;

CREATE TABLE t_dynamic_left (k Dynamic) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dynamic_right (k Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dynamic_left SELECT CAST(if(number = 0, NULL, 10), 'Dynamic') FROM numbers(2);
INSERT INTO t_dynamic_right SELECT CAST(if(number = 0, NULL, 10), 'Dynamic') FROM numbers(2);

-- A Dynamic NULL is the same discriminator value, so the same guard has to survive here.
SELECT 'dynamic inner', count()
FROM t_dynamic_left AS l INNER JOIN t_dynamic_right AS r
    ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL;

SELECT 'dynamic guard kept', count() FROM ( EXPLAIN QUERY TREE
    SELECT count() FROM t_dynamic_left AS l INNER JOIN t_dynamic_right AS r
        ON l.k = r.k AND l.k IS NOT NULL AND r.k IS NOT NULL
) WHERE explain ILIKE '%function_name: isNotNull%';

DROP TABLE t_variant_left;
DROP TABLE t_variant_right;
DROP TABLE t_nullable_left;
DROP TABLE t_nullable_right;
DROP TABLE t_lc_left;
DROP TABLE t_lc_right;
DROP TABLE t_plain_left;
DROP TABLE t_plain_right;
DROP TABLE t_dynamic_left;
DROP TABLE t_dynamic_right;
