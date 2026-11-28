-- Index analysis reads `ifNull(x, ...)` and `coalesce(x, ...)` over a non-Nullable `x` as `x`, also inside a key
-- expression such as `sipHash64(...)`. See ClickHouse/ClickHouse#110117.

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

SELECT count() > 0 AS prewhere_prunes
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident PREWHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)))
WHERE explain ILIKE '%Parts: 1/8%';

-- Results must be identical with and without the wrapper.
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(p) = sipHash64(toUInt8(3));
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3));
SELECT sum(k) FROM t_nested_ident WHERE sipHash64(coalesce(p, 0)) = sipHash64(toUInt8(3));

-- Gated on allow_key_condition_coalesce_rewrite: with the setting off the wrapper does not prune,
-- while the bare column still does.
SELECT count() > 0 AS ifnull_no_prune_when_off
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)) SETTINGS allow_key_condition_coalesce_rewrite = 0)
WHERE explain ILIKE '%Parts: 8/8%';

SELECT count() > 0 AS bare_still_prunes_when_off
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_nested_ident WHERE sipHash64(p) = sipHash64(toUInt8(3)) SETTINGS allow_key_condition_coalesce_rewrite = 0)
WHERE explain ILIKE '%Parts: 1/8%';

DROP TABLE t_nested_ident;

-- The primary key and a `minmax` index on the bare expression are used too.
DROP TABLE IF EXISTS t_nested_ident_key;

CREATE TABLE t_nested_ident_key (p UInt8, k UInt32, INDEX i sipHash64(p) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY sipHash64(p)
SETTINGS index_granularity = 64;

INSERT INTO t_nested_ident_key
SELECT toUInt8(intDiv(number, 1024)), toUInt32(number % 1024)
FROM numbers(8192) SETTINGS max_insert_threads = 1;

SELECT sum(k) FROM t_nested_ident_key WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)) SETTINGS force_primary_key = 1;
SELECT sum(k) FROM t_nested_ident_key WHERE sipHash64(coalesce(p, 0)) = sipHash64(toUInt8(3)) SETTINGS force_data_skipping_indices = 'i';

DROP TABLE t_nested_ident_key;

-- Keys that contain the wrapper themselves are still matched by the same wrapped expression.
DROP TABLE IF EXISTS t_wrapped_key;

CREATE TABLE t_wrapped_key (p UInt8, k UInt32, INDEX i sipHash64(ifNull(p, 0)) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY sipHash64(ifNull(p, 0)) ORDER BY sipHash64(coalesce(p, 0))
SETTINGS index_granularity = 64;

INSERT INTO t_wrapped_key
SELECT toUInt8(intDiv(number, 1024)), toUInt32(number % 1024)
FROM numbers(8192) SETTINGS max_insert_threads = 1;

SELECT sum(k) FROM t_wrapped_key WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)) SETTINGS force_index_by_date = 1;
SELECT sum(k) FROM t_wrapped_key WHERE sipHash64(coalesce(p, 0)) = sipHash64(toUInt8(3)) SETTINGS force_primary_key = 1;
SELECT sum(k) FROM t_wrapped_key WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(3)) SETTINGS force_data_skipping_indices = 'i';

DROP TABLE t_wrapped_key;

-- Over a Nullable column the wrapper maps NULL to the fallback, so it is kept: the 100 rows with p = NULL
-- match sipHash64(ifNull(p, 0)) = sipHash64(0) and their partition must not be pruned.
DROP TABLE IF EXISTS t_nullable_ident;

CREATE TABLE t_nullable_ident (p Nullable(UInt8), k UInt32)
ENGINE = MergeTree PARTITION BY sipHash64(p) ORDER BY k
SETTINGS index_granularity = 64, allow_nullable_key = 1;

INSERT INTO t_nullable_ident
SELECT toUInt8(intDiv(number, 1024)), toUInt32(number % 1024)
FROM numbers(8192) SETTINGS max_insert_threads = 1;
INSERT INTO t_nullable_ident SELECT NULL, number FROM numbers(100);

SELECT count() FROM t_nullable_ident WHERE sipHash64(ifNull(p, 0)) = sipHash64(toUInt8(0));

DROP TABLE t_nullable_ident;

-- The query itself is not changed: `arrayJoin(a)` and `arrayJoin(ifNull(a, []))` are two expansions.
DROP TABLE IF EXISTS t_ident_array_join;

CREATE TABLE t_ident_array_join (a Array(UInt8)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_ident_array_join VALUES ([1, 2]);

SELECT count() FROM t_ident_array_join WHERE arrayJoin(a) > 0 AND arrayJoin(ifNull(a, [])) > 0;
SELECT count() FROM (SELECT arrayJoin(a) AS x FROM t_ident_array_join WHERE arrayJoin(coalesce(a, [])) > 0);

DROP TABLE t_ident_array_join;
