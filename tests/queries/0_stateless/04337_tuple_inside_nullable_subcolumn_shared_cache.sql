-- { echo }
-- Read a `Nullable(Tuple(...))` subcolumn together with its descendant subcolumns in one
-- query, so that the descendant's nested data is served from the substreams cache (a column
-- shared with another subcolumn read, containing rows from multiple ranges).

SET allow_experimental_nullable_tuple_type = 1;

-- Wide part, one row per granule.
DROP TABLE IF EXISTS t_shared_cache_wide;
CREATE TABLE t_shared_cache_wide (x Tuple(a Nullable(Tuple(b Nullable(UInt32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_shared_cache_wide SELECT number % 2 ? tuple(NULL) : tuple(tuple(number)) FROM numbers(4);
SELECT x.a, x.a.b FROM t_shared_cache_wide;
SELECT x.a.b, x.a FROM t_shared_cache_wide;
SELECT x, x.a, x.a.b, isNull(x.a), isNull(x.a.b) FROM t_shared_cache_wide;
SELECT x.a, x.a.b FROM t_shared_cache_wide SETTINGS max_block_size = 1;
DROP TABLE t_shared_cache_wide;

-- Compact part.
DROP TABLE IF EXISTS t_shared_cache_compact;
CREATE TABLE t_shared_cache_compact (x Tuple(a Nullable(Tuple(b Nullable(UInt32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_shared_cache_compact SELECT number % 2 ? tuple(NULL) : tuple(tuple(number)) FROM numbers(4);
SELECT x.a, x.a.b FROM t_shared_cache_compact;
SELECT x.a.b, x.a FROM t_shared_cache_compact;
SELECT x, x.a, x.a.b, isNull(x.a), isNull(x.a.b) FROM t_shared_cache_compact;
DROP TABLE t_shared_cache_compact;

-- Both levels Nullable: the subcolumn serializations are nested parent-null-map wrappers.
DROP TABLE IF EXISTS t_shared_cache_nested;
CREATE TABLE t_shared_cache_nested (id UInt8, x Nullable(Tuple(a Nullable(Tuple(b Nullable(UInt32)))))) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_shared_cache_nested VALUES (0, NULL), (1, (NULL)), (2, ((NULL))), (3, ((42)));
SELECT id, x, x.a, x.a.b, isNull(x.a), isNull(x.a.b) FROM t_shared_cache_nested ORDER BY id;
SELECT id, x.a.b, x.a FROM t_shared_cache_nested ORDER BY id;
SELECT count(x.a), count(x.a.b) FROM t_shared_cache_nested;
DROP TABLE t_shared_cache_nested;
