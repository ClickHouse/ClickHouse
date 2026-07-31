-- Tags: no-old-analyzer

-- A constant conjunct of the ON expression belongs to neither side, so it is grouped with the left
-- one when conditions are pushed out of the join. No filter can be built from a constant predicate,
-- and the conjunct used to be dropped instead of staying in the ON expression.
-- `query_plan_short_circuit_constant_false_join = 0` keeps the constant-false join on the general path.

SET query_plan_short_circuit_constant_false_join = 0;

DROP TABLE IF EXISTS t_04648;
CREATE TABLE t_04648 (id UInt32, v Int64, pk String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04648 SELECT number, number * 2, ['aa', 'bb', 'cc'][number % 3 + 1] FROM numbers(10);

SELECT 'inner';
SELECT l.* FROM t_04648 AS l INNER JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8))
WHERE l.pk = 'cc' AND r.id >= 3;

SELECT 'right';
SELECT l.* FROM t_04648 AS l RIGHT JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8))
WHERE l.pk = 'cc' AND r.id >= 3;

SELECT 'variant';
SELECT count() FROM t_04648 AS l INNER JOIN t_04648 AS r
ON l.id = r.id AND CAST(CAST(NULL AS Variant(UInt8, String)) AS Nullable(UInt8))
SETTINGS enable_variant_type = 1;

SELECT 'plan';
SELECT extract(explain, 'Join conditions:.*') AS cond FROM (
    EXPLAIN SELECT l.* FROM t_04648 AS l INNER JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8))
    WHERE l.pk = 'cc' AND r.id >= 3
) WHERE cond != '';

SELECT 'kinds';
SELECT 'inner', count() FROM (SELECT l.id FROM t_04648 AS l INNER JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'left', count() FROM (SELECT l.id FROM t_04648 AS l LEFT JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'right', count() FROM (SELECT r.id FROM t_04648 AS l RIGHT JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'full', count() FROM (SELECT l.id FROM t_04648 AS l FULL JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'left semi', count() FROM (SELECT l.id FROM t_04648 AS l LEFT SEMI JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'right semi', count() FROM (SELECT r.id FROM t_04648 AS l RIGHT SEMI JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'left anti', count() FROM (SELECT l.id FROM t_04648 AS l LEFT ANTI JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));
SELECT 'right anti', count() FROM (SELECT r.id FROM t_04648 AS l RIGHT ANTI JOIN t_04648 AS r ON l.id = r.id AND CAST(NULL AS Nullable(UInt8)));

-- A one-sided ON predicate must still be pushed out of the join, alone or next to the constant.
SELECT 'pushdown';
SELECT count() FROM t_04648 AS l INNER JOIN t_04648 AS r ON l.id = r.id AND l.pk = 'cc';
SELECT count() FROM t_04648 AS l INNER JOIN t_04648 AS r ON l.id = r.id AND l.pk = 'cc' AND CAST(NULL AS Nullable(UInt8));

DROP TABLE t_04648;
