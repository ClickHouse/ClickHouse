-- Subcolumns whose serialization is produced by a type-dependent subcolumn creator, read from
-- both part formats: the reader takes them from the part's shared serialization cache.
SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_subcolumns_cache_wide;
DROP TABLE IF EXISTS t_subcolumns_cache_compact;

CREATE TABLE t_subcolumns_cache_wide
(
    id UInt32,
    nt Nullable(Tuple(a UInt8, b String)),
    v Variant(UInt8, String, Array(UInt8)),
    sp Nullable(UInt64),
    arr Array(Nullable(Tuple(x UInt8, y Nullable(String)))),
    m Map(String, Nullable(UInt32)),
    n Nested(k UInt32, s Nullable(String))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5;

CREATE TABLE t_subcolumns_cache_compact AS t_subcolumns_cache_wide
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_subcolumns_cache_wide SELECT
    number,
    if(number % 3 = 0, NULL, (toUInt8(number), toString(number))),
    multiIf(number % 3 = 0, toUInt8(number)::Variant(UInt8, String, Array(UInt8)), number % 3 = 1, toString(number)::Variant(UInt8, String, Array(UInt8)), [toUInt8(number)]::Variant(UInt8, String, Array(UInt8))),
    if(number % 10 = 0, number, 0),
    [if(number % 2 = 0, NULL, (toUInt8(number), if(number % 4 = 1, NULL, 'y'))), (1, 'z')],
    map('a', if(number % 2 = 0, NULL, toUInt32(number))),
    arrayMap(i -> toUInt32(i), range(number % 3)),
    arrayMap(i -> if(i = 0, NULL, toString(i)), range(number % 3))
FROM numbers(1000);

INSERT INTO t_subcolumns_cache_compact SELECT * FROM t_subcolumns_cache_wide;

SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_subcolumns_cache_%' AND active GROUP BY part_type ORDER BY part_type;

SELECT 'wide';
SELECT sum(nt.a), sum(length(nt.b)), countIf(isNull(nt)), countIf(isNull(nt.a)) FROM t_subcolumns_cache_wide;
SELECT sum(v.UInt8), sum(length(v.String)), sum(length(v.`Array(UInt8)`)), sum(v.`Array(UInt8)`.size0), countIf(isNull(v.UInt8)) FROM t_subcolumns_cache_wide;
SELECT sum(sp), sum(sp.null), countIf(sp != 0) FROM t_subcolumns_cache_wide;
SELECT sum(length(arr.x)), sum(arraySum(arrayMap(t -> ifNull(t, 0), arr.x))), sum(length(arr.y)), sum(arr.size0), sum(arraySum(arr.null)) FROM t_subcolumns_cache_wide;
SELECT sum(length(m.keys)), sum(arraySum(m.values.null)), sum(m.size0) FROM t_subcolumns_cache_wide;
SELECT sum(arraySum(n.k)), sum(length(n.s)), sum(n.k.size0), sum(arraySum(n.s.null)) FROM t_subcolumns_cache_wide;
SELECT id, nt.b, v.String, arr.y, n.s FROM t_subcolumns_cache_wide WHERE id IN (1, 4, 7) ORDER BY id;
SELECT sum(nt.a) FROM t_subcolumns_cache_wide PREWHERE sp.null = 0 WHERE v.UInt8 IS NOT NULL;

SELECT 'compact';
SELECT sum(nt.a), sum(length(nt.b)), countIf(isNull(nt)), countIf(isNull(nt.a)) FROM t_subcolumns_cache_compact;
SELECT sum(v.UInt8), sum(length(v.String)), sum(length(v.`Array(UInt8)`)), sum(v.`Array(UInt8)`.size0), countIf(isNull(v.UInt8)) FROM t_subcolumns_cache_compact;
SELECT sum(sp), sum(sp.null), countIf(sp != 0) FROM t_subcolumns_cache_compact;
SELECT sum(length(arr.x)), sum(arraySum(arrayMap(t -> ifNull(t, 0), arr.x))), sum(length(arr.y)), sum(arr.size0), sum(arraySum(arr.null)) FROM t_subcolumns_cache_compact;
SELECT sum(length(m.keys)), sum(arraySum(m.values.null)), sum(m.size0) FROM t_subcolumns_cache_compact;
SELECT sum(arraySum(n.k)), sum(length(n.s)), sum(n.k.size0), sum(arraySum(n.s.null)) FROM t_subcolumns_cache_compact;
SELECT id, nt.b, v.String, arr.y, n.s FROM t_subcolumns_cache_compact WHERE id IN (1, 4, 7) ORDER BY id;
SELECT sum(nt.a) FROM t_subcolumns_cache_compact PREWHERE sp.null = 0 WHERE v.UInt8 IS NOT NULL;

DROP TABLE t_subcolumns_cache_wide;
DROP TABLE t_subcolumns_cache_compact;
