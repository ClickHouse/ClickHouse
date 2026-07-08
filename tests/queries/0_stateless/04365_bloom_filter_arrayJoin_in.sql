-- Tags: no-parallel-replicas
-- Bloom filter skip index on an Array column must be used for `arrayJoin(col) IN (set)`
-- (and `GLOBAL IN`), the same way it is already used for `hasAny(col, set)`, and for
-- `arrayJoin(col) = const`, the same way it is already used for `has(col, const)`.
-- Issues: https://github.com/ClickHouse/ClickHouse/issues/109516
--         https://github.com/ClickHouse/ClickHouse/issues/109844

DROP TABLE IF EXISTS t_arrayjoin_bf;

CREATE TABLE t_arrayjoin_bf
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
-- Pin granule layout (query-level SETTINGS override CI-randomized merge tree settings) so the
-- Granules: X/Y counts below are deterministic: 100000 rows / 8192 = 13 granules.
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Each tag is unique, so a given tag lives in exactly one granule: pruning is observable.
INSERT INTO t_arrayjoin_bf SELECT number, [concat('tag_', toString(number))] FROM numbers(100000);

-- Baseline: hasAny prunes to the single matching granule (a bloom filter false positive keeps one extra -> 2/13).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE hasAny(tags, ['tag_42'])) WHERE explain ILIKE '%Granules: 2/13%';

-- arrayJoin(tags) IN (const set) now uses the index and prunes identically to hasAny.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) IN ('tag_42')) WHERE explain ILIKE '%Granules: 2/13%';

-- arrayJoin(tags) IN (subquery set).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) IN (SELECT 'tag_42')) WHERE explain ILIKE '%Granules: 2/13%';

-- arrayJoin(tags) GLOBAL IN (subquery set).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) GLOBAL IN (SELECT 'tag_42')) WHERE explain ILIKE '%Granules: 2/13%';

-- Multi-element set: two tags in two distinct granules -> more granules read than single-element.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) IN ('tag_42', 'tag_99999')) WHERE explain ILIKE '%Granules: 3/13%';

-- Safety: NOT IN must NOT prune (a granule with the set element can still yield rows outside the set).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) NOT IN ('tag_42')) WHERE explain ILIKE '%Granules: 13/13%';

-- arrayJoin(tags) = const now uses the index and prunes identically to has(tags, const).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) = 'tag_42') WHERE explain ILIKE '%Granules: 2/13%';

-- Safety: != must NOT prune (a granule with the value can still yield rows whose arrayJoined value differs).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) != 'tag_42') WHERE explain ILIKE '%Granules: 13/13%';

-- Correctness: results are unaffected by index usage.
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) IN ('tag_42', 'tag_99999');
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) NOT IN ('tag_42');
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) = 'tag_42';
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) != 'tag_42';

DROP TABLE t_arrayjoin_bf;

-- Default value in the set. The rewrite fires only for the inner `arrayJoin(col)` function in
-- WHERE, where empty arrays produce no rows, so `hasAny` semantics are exact even when the set
-- contains the element type's default. A granule of all-empty arrays yields no row (result 0),
-- while a granule that actually contains the default value as a real element is kept and its
-- rows are returned. Results must be identical with the skip index on and off.
DROP TABLE IF EXISTS t_arrayjoin_bf_default;

CREATE TABLE t_arrayjoin_bf_default
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- First 5000 rows: arrays that really contain the default value '' as an element.
INSERT INTO t_arrayjoin_bf_default SELECT number, ['', concat('x_', toString(number))] FROM numbers(5000);
-- Remaining rows: unique non-default tags, no empty string.
INSERT INTO t_arrayjoin_bf_default SELECT number + 5000, [concat('tag_', toString(number))] FROM numbers(95000);

-- Default value is a real element in one granule -> pruning still fires (2/13) and the result is correct.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) IN ('')) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) IN ('') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) IN ('') SETTINGS use_skip_indexes = 0;
-- Equality form with the default value: the analyzer rewrites `s = ''` into `empty(s)`, so the
-- derivation does not fire (it keys off `equals`) and no granule is skipped. This is the safe
-- fallback -> full scan, correct result. Results must be identical with the skip index on and off.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '') WHERE explain ILIKE '%Granules: 13/13%';
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '' SETTINGS use_skip_indexes = 0;
-- A non-default value that is a real element in one granule -> pruning fires (1/13), result correct.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1') WHERE explain ILIKE '%Granules: 1/13%';
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1' SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_default;

-- LEFT ARRAY JOIN safety. A LEFT ARRAY JOIN expands an empty array into one row carrying the
-- element type's default value, so `col IN (default)` can match a row produced from an empty
-- array. That predicate lives above the ARRAY JOIN step and is never pushed into the skip index
-- (the rewrite only applies to the inner arrayJoin function in WHERE), so results are correct and
-- identical with the skip index on and off. The table has a whole granule of empty arrays.
DROP TABLE IF EXISTS t_arrayjoin_bf_left;

CREATE TABLE t_arrayjoin_bf_left
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_arrayjoin_bf_left SELECT number, [] FROM numbers(20000);
INSERT INTO t_arrayjoin_bf_left SELECT number + 20000, [concat('tag_', toString(number))] FROM numbers(80000);

-- 20000 empty-array rows are each expanded to one default-value row -> 20000 matches, index or not.
SELECT count() FROM t_arrayjoin_bf_left LEFT ARRAY JOIN tags WHERE tags IN ('') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_left LEFT ARRAY JOIN tags WHERE tags IN ('') SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_left;
