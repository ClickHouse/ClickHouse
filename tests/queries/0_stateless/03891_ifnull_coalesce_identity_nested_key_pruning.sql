-- Nested identity wrapper inside a key expression: ifNull(x, c) / coalesce(x, c) over a
-- non-Nullable x is an identity, so partition/primary-key pruning must match it the same as
-- the bare column, even when nested inside another key function (e.g. sipHash64(ifNull(p, 0))).
-- See ClickHouse/ClickHouse#109998 (follow-up from @clickgapai).

-- The simplification is a query tree pass, so it only runs with the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_nested_ident;

CREATE TABLE t_nested_ident (p UInt8, k UInt32)
ENGINE = MergeTree PARTITION BY sipHash64(p) ORDER BY k
SETTINGS index_granularity = 64;

INSERT INTO t_nested_ident
SELECT toUInt8(intDiv(number, 1024)), toUInt32(number % 1024)
FROM numbers(8192) SETTINGS max_insert_threads = 1;

-- Baseline: bare column prunes to a single part.
SELECT count() > 0 AS bare_prunes
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident WHERE sipHash64(p) = sipHash64(toUInt8(3)))
WHERE explain ILIKE '%Parts: 1/8%';

-- ifNull / coalesce wrappers over the non-Nullable column must prune identically.
SELECT count() > 0 AS ifnull_prunes
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)))
WHERE explain ILIKE '%Parts: 1/8%';

SELECT count() > 0 AS coalesce_prunes
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident WHERE sipHash64(coalesce(p, 0)) = sipHash64(toUInt8(3)))
WHERE explain ILIKE '%Parts: 1/8%';

-- Results must be identical with and without the wrapper.
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(p) = sipHash64(toUInt8(3));
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3));
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(coalesce(p, 0)) = sipHash64(toUInt8(3));

DROP TABLE t_nested_ident;

-- Safety: over a Nullable column the wrapper is NOT an identity (NULL -> fallback) and must
-- be preserved. Rows with p = NULL fall into the partition of ifNull(NULL, 0) = 0 and must be
-- counted for sipHash64(ifNull(p, 0)) = sipHash64(0).
DROP TABLE IF EXISTS t_nullable_ident;

CREATE TABLE t_nullable_ident (p Nullable(UInt8), k UInt32)
ENGINE = MergeTree PARTITION BY sipHash64(ifNull(p, 0)) ORDER BY k
SETTINGS index_granularity = 64, allow_nullable_key = 1;

INSERT INTO t_nullable_ident
SELECT toUInt8(intDiv(number, 1024)), toUInt32(number % 1024)
FROM numbers(8192) SETTINGS max_insert_threads = 1;
INSERT INTO t_nullable_ident SELECT NULL, number FROM numbers(100);

-- 1024 rows with p = 0 plus 100 rows with p = NULL (mapped to 0) = 1124.
SELECT count() FROM t_nullable_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(0));

DROP TABLE t_nullable_ident;
