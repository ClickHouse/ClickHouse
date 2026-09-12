SET allow_suspicious_variant_types = 1;

SELECT '-- declared type does not depend on variant_throw_on_type_mismatch';

SELECT toTypeName(max2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8)') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(max2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8)') AS v) SETTINGS variant_throw_on_type_mismatch = 0;
SELECT toTypeName(max2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8, Int128)') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(max2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8, Int128)') AS v) SETTINGS variant_throw_on_type_mismatch = 0;
SELECT toTypeName(min2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8, String)') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(min2(v, v)) FROM (SELECT CAST(1, 'Variant(UInt8, String)') AS v) SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- rows of an incompatible alternative are NULL, matching the declared type';

SELECT toTypeName(max2(v, v)), max2(v, v), isNull(max2(v, v))
FROM (SELECT CAST(44, 'Variant(UInt8, String)') AS v UNION ALL SELECT CAST('zz', 'Variant(UInt8, String)') AS v)
ORDER BY 3
SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- a result type that cannot be inside Nullable keeps the Variant wrapper';

SELECT toTypeName(mapFromArrays(a, b)), mapFromArrays(a, b), isNull(mapFromArrays(a, b))
FROM (
    SELECT CAST([1, 2], 'Variant(Array(UInt8), UInt8)') AS a, CAST(['x', 'y'], 'Variant(Array(String), String)') AS b
    UNION ALL
    SELECT CAST(7, 'Variant(Array(UInt8), UInt8)') AS a, CAST('z', 'Variant(Array(String), String)') AS b
)
ORDER BY 3
SETTINGS variant_throw_on_type_mismatch = 0;

SELECT toTypeName(arrayConcat(a, a)), arrayConcat(a, a), isNull(arrayConcat(a, a))
FROM (SELECT CAST([1, 2], 'Variant(Array(UInt8), UInt8)') AS a UNION ALL SELECT CAST(7, 'Variant(Array(UInt8), UInt8)') AS a)
ORDER BY 3
SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- an alternative that resolves to Nothing without a type error is kept';

SELECT toTypeName(arrayElement(v, 1)) FROM (SELECT CAST([1, 2], 'Variant(Array(Nothing), Array(UInt8))') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(arrayElement(v, 1)) FROM (SELECT CAST([1, 2], 'Variant(Array(Nothing), Array(UInt8))') AS v) SETTINGS variant_throw_on_type_mismatch = 0;

SELECT toTypeName(arrayElementOrNull(v, 1)) FROM (SELECT CAST([1, 2], 'Variant(Array(Nothing), Array(UInt8))') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(arrayElementOrNull(v, 1)) FROM (SELECT CAST([1, 2], 'Variant(Array(Nothing), Array(UInt8))') AS v) SETTINGS variant_throw_on_type_mismatch = 0;

SELECT toTypeName(arrayElementOrNull(v, 1)) FROM (SELECT CAST(map(1, 'x'), 'Variant(Map(UInt8, Nothing), Map(UInt8, String))') AS v) SETTINGS variant_throw_on_type_mismatch = 1;
SELECT toTypeName(arrayElementOrNull(v, 1)) FROM (SELECT CAST(map(1, 'x'), 'Variant(Map(UInt8, Nothing), Map(UInt8, String))') AS v) SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- every alternative incompatible';

SELECT toTypeName(max2(v, v)) FROM (SELECT CAST('x', 'Variant(String, IPv4)') AS v) SETTINGS variant_throw_on_type_mismatch = 0;
SELECT toTypeName(max2(v, v)) FROM (SELECT CAST('x', 'Variant(String, IPv4)') AS v) SETTINGS variant_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Dynamic is unaffected';

SELECT toTypeName(max2(d, d)), max2(d, d) FROM (SELECT CAST(5, 'Dynamic') AS d) SETTINGS variant_throw_on_type_mismatch = 0, dynamic_throw_on_type_mismatch = 0;
SELECT toTypeName(max2(d, d)), max2(d, d) FROM (SELECT CAST(5, 'Dynamic') AS d) SETTINGS variant_throw_on_type_mismatch = 1, dynamic_throw_on_type_mismatch = 1;

SELECT '-- a custom-named result type is unaffected';

SELECT toTypeName(readWKTPoint(v)) FROM (SELECT CAST('POINT(1 2)', 'Variant(String, UInt8)') AS v) SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- INSERT with such an expression in the key writes both part types';

SET variant_throw_on_type_mismatch = 1;

DROP TABLE IF EXISTS t_04927_compact;
DROP TABLE IF EXISTS t_04927_wide;
DROP TABLE IF EXISTS t_04927_skip;

CREATE TABLE t_04927_compact (c0 Nullable(Int32), c2 Variant(UInt8, Time, UInt128, Decimal256(13), Int128))
ENGINE = MergeTree ORDER BY (max2(c2, c2), c0) PRIMARY KEY (max2(c2, c2))
SETTINGS index_granularity = 2, allow_nullable_key = 1, allow_suspicious_indices = 1, min_bytes_for_wide_part = 1000000000;

CREATE TABLE t_04927_wide (c0 Nullable(Int32), c2 Variant(UInt8, Time, UInt128, Decimal256(13), Int128))
ENGINE = MergeTree ORDER BY (max2(c2, c2), c0) PRIMARY KEY (max2(c2, c2))
SETTINGS index_granularity = 2, allow_nullable_key = 1, allow_suspicious_indices = 1, min_bytes_for_wide_part = 0;

CREATE TABLE t_04927_skip (c0 Nullable(Int32), c2 Variant(UInt8, String), INDEX idx max2(c2, c2) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY c0
SETTINGS allow_nullable_key = 1, allow_suspicious_indices = 1, index_granularity = 1;

SET variant_throw_on_type_mismatch = 0;

INSERT INTO t_04927_compact (c2, c0) VALUES (44, 1), (7, 2), (99, 3);
INSERT INTO t_04927_wide (c2, c0) VALUES (44, 1), (7, 2), (99, 3);
INSERT INTO t_04927_skip (c2, c0) VALUES (44, 1), (7, 2), (99, 3), ('zz', 4);

SELECT 'compact', count() FROM t_04927_compact;
SELECT 'wide', count() FROM t_04927_wide;
SELECT 'skip index', count() FROM t_04927_skip;
SELECT table, part_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_04927_compact', 't_04927_wide') AND active
ORDER BY table;

SELECT 'skip index prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04927_skip WHERE max2(c2, c2) = 44)
WHERE explain ILIKE '%Granules: 1/4%';

SYSTEM STOP MERGES t_04927_compact;
INSERT INTO t_04927_compact (c2, c0) VALUES (55, 4);
SELECT 'parts before merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_04927_compact' AND active;
SYSTEM START MERGES t_04927_compact;
OPTIMIZE TABLE t_04927_compact FINAL;
SELECT 'parts after merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_04927_compact' AND active;
SELECT 'after merge', count() FROM t_04927_compact;

DETACH TABLE t_04927_compact;
ATTACH TABLE t_04927_compact;
INSERT INTO t_04927_compact (c2, c0) VALUES (66, 5);
SELECT 'after reattach', count() FROM t_04927_compact;

SELECT '-- a set index over a kept Nothing alternative survives a reload';

DROP TABLE IF EXISTS t_04927_nothing_idx;

CREATE TABLE t_04927_nothing_idx (c0 Int32, v Variant(Array(Nothing), Array(UInt8)),
    INDEX idx arrayElementOrNull(v, 1) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY c0
SETTINGS index_granularity = 1, allow_suspicious_indices = 1;

INSERT INTO t_04927_nothing_idx VALUES (1, CAST([10, 20], 'Variant(Array(Nothing), Array(UInt8))')),
                                       (2, CAST([30, 40], 'Variant(Array(Nothing), Array(UInt8))')),
                                       (3, CAST([50, 60], 'Variant(Array(Nothing), Array(UInt8))'));

DETACH TABLE t_04927_nothing_idx;
ATTACH TABLE t_04927_nothing_idx;

SELECT 'nothing index type', toTypeName(arrayElementOrNull(v, 1)) FROM t_04927_nothing_idx LIMIT 1;
SELECT 'nothing index match', count() FROM t_04927_nothing_idx WHERE arrayElementOrNull(v, 1) = 30;
SELECT 'nothing index values', groupArray(arrayElementOrNull(v, 1)) FROM t_04927_nothing_idx;
SELECT 'nothing index prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_04927_nothing_idx WHERE arrayElementOrNull(v, 1) = 30)
WHERE explain ILIKE '%Granules: 1/3%';

DROP TABLE t_04927_nothing_idx;
DROP TABLE t_04927_compact;
DROP TABLE t_04927_wide;
DROP TABLE t_04927_skip;
