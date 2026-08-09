DROP TABLE IF EXISTS t_index_hint;
DROP TABLE IF EXISTS t_index_hint_part;

CREATE TABLE t_index_hint (g UInt16, id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_index_hint SELECT number % 10, number FROM numbers(1000);

SELECT 'no hint', count() FROM t_index_hint WHERE id >= 1 AND id <= 3;

-- A truthy constant that does not fit UInt8 must not exclude any row: all of these returned 0
-- (or threw) because the argument was narrowed with a cast to UInt8.
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(256);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(512);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(65536);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(-256);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0.5);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(-0.5);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt16(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(toNullable(256)));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(256, 512);

-- Types with no boolean interpretation contribute no filter (WHERE rejects them outright).
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(256));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toInt128(256));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(256, 0));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x');
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint([1, 2]);
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint((1, 2));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x', 256);
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(256, 0), 256);

-- A NULL hint is not true, so pruning everything is correct; it must not throw.
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(NULL);
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(CAST(NULL, 'Nullable(UInt8)'));
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(NULL, 1);

-- A falsy hint must still prune everything (pinned by 02841/02892/02962).
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(0));
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(0));
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0.0);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0, 256);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x', 0);

-- Under GROUPING SETS the wrongly emptied scan still emits the ()-set row, so the wrong answer is
-- a single non-empty row rather than an empty result.
SELECT 'grouping', g, count(), grouping(g) FROM t_index_hint
WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(256))
GROUP BY GROUPING SETS ((g), ()) ORDER BY g;

-- Granule pruning is still applied for a hint that carries a usable condition. The range sits
-- INSIDE the hint so the hint is the only possible source of pruning; the second statement is the
-- negative control, where a hint carrying no usable condition must leave every granule.
-- optimize_use_implicit_projections = 0 as in 01739_index_hint and 02880_indexHint__partition_id,
-- and optimize_trivial_count_query = 0 as well: either one lets count() be answered without
-- consulting the index, and then EXPLAIN prints no Granules line at all.
-- countIf, not a WHERE over the subquery: without the analyzer the extract() is evaluated before
-- the LIKE, so a non-matching row reaches toUInt64 and throws.
SELECT 'granules pruned', countIf(
    toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'))) > 0
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_index_hint WHERE indexHint(id >= 1 AND id <= 3)
    SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
);

SELECT 'granules kept', countIf(explain LIKE '%Granules: %/%'
    AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) = toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'))) > 0
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_index_hint WHERE indexHint(1)
    SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
);

-- Part pruning by a virtual column is still applied. indexHint is row-level TRUE, so a count below
-- the table total can only come from parts being dropped during analysis.
CREATE TABLE t_index_hint_part (p UInt8, id UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_index_hint_part SELECT number % 4, number FROM numbers(400);

SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0');
SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0', 256);
SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0', 'x');
SELECT 'parts kept', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(256);
SELECT 'parts kept', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint('x');

-- Two hints on an allowed input keep two rewritten children under the enclosing `and`, which reuses
-- its parent node and so its UInt8 result type. Answering 200 rather than 300 or 400 is what proves
-- both children survived, so these also cover the case where one of them is Nullable.
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(_partition_id != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toLowCardinality(toNullable(_partition_id)) != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1', 256);
SELECT 'three hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1')
  AND indexHint(_partition_id != '2');

-- A hint over a Nullable column whose value really is NULL for some parts: NULL is not true, so
-- those parts are pruned, and converting must not throw on the NULL.
SELECT 'nullable column', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(if(_partition_id = '0', NULL, 1));

-- The sibling `and` branch of the same code path must be unaffected.
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 256;
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND toNullable(256);
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND toLowCardinality(toNullable(256));
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 0.5;
SELECT 'and branch falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 0;

DROP TABLE t_index_hint;
DROP TABLE t_index_hint_part;
