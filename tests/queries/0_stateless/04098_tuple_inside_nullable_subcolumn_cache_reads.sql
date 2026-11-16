-- { echo }
-- Verify repeated reads of descendant subcolumns extracted from `Nullable(Tuple(...))`.
-- Uses many one-row granules and repeated `.null` / `.size` reads in one query to
-- exercise the substreams cache path across multiple ranges.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple_cache;
CREATE TABLE t_nullable_tuple_cache
(
    key UInt64,
    tup Nullable(Tuple(`a.b` Nullable(String), s Nullable(String), payload String))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple_cache VALUES
    (0, NULL),
    (1, (NULL, 's1', 'p1')),
    (2, ('a2', NULL, 'p2')),
    (3, ('a3', 's3', 'p3')),
    (4, ('a4', 's4', 'p4')),
    (5, NULL),
    (6, (NULL, 's6', 'p6')),
    (7, ('a7', NULL, 'p7')),
    (8, ('a8', 's8', 'p8')),
    (9, ('a9', 's9', 'p9')),
    (10, NULL),
    (11, (NULL, 's11', 'p11'));

SELECT key, tup.s, coalesce(tup.s.null, toUInt8(1)), coalesce(tup.s.null, toUInt8(1)), ifNull(tup.s.size, toUInt64(999)), ifNull(tup.s.size, toUInt64(999)), tup.`a.b`, coalesce(tup.`a.b`.null, toUInt8(1)), ifNull(tup.`a.b`.size, toUInt64(999)) FROM t_nullable_tuple_cache WHERE key IN (0, 1, 2, 3, 5, 6, 7, 8, 10, 11) ORDER BY key;
SELECT key, tup.`a.b`, coalesce(tup.`a.b`.null, toUInt8(1)), tup.s, coalesce(tup.s.null, toUInt8(1)), ifNull(tup.`a.b`.size, toUInt64(999)), ifNull(tup.s.size, toUInt64(999)) FROM t_nullable_tuple_cache WHERE key IN (11, 10, 8, 7, 6, 5, 3, 2, 1, 0) ORDER BY key DESC;

DROP TABLE t_nullable_tuple_cache;
