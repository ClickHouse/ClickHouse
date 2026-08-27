-- Tags: no-parallel-replicas
-- `arrayJoin(col) IN (set)` and `arrayJoin(col) = const` must use the Array bloom filter index,
-- like `hasAny(col, set)` and `has(col, const)` already do.
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
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) NOT IN ('tag_42')) WHERE explain ILIKE '%Name: idx_tags%';

-- arrayJoin(tags) = const now uses the index and prunes identically to has(tags, const).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) = 'tag_42') WHERE explain ILIKE '%Granules: 2/13%';

-- Safety: != must NOT prune (a granule with the value can still yield rows whose arrayJoined value differs).
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) != 'tag_42') WHERE explain ILIKE '%Name: idx_tags%';

-- Correctness: results are unaffected by index usage.
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) IN ('tag_42', 'tag_99999');
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) NOT IN ('tag_42');
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) = 'tag_42';
SELECT count() FROM t_arrayjoin_bf WHERE arrayJoin(tags) != 'tag_42';

DROP TABLE t_arrayjoin_bf;

-- An empty array produces no row for the inner `arrayJoin(col)`, so only a granule holding the
-- default as a real element is kept.
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
-- Pinned: with `optimize_empty_string_comparisons = 1` the predicate becomes `empty(s)`, which no
-- longer reaches the derivation.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain ILIKE '%Granules: 2/13%';
-- Results are identical with the skip index on and off.
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '' SETTINGS use_skip_indexes = 1, optimize_empty_string_comparisons = 0;
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = '' SETTINGS use_skip_indexes = 0, optimize_empty_string_comparisons = 0;
-- A non-default value that is a real element in one granule -> pruning fires (1/13), result correct.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1') WHERE explain ILIKE '%Granules: 1/13%';
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_default WHERE arrayJoin(tags) = 'x_1' SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_default;

-- LEFT ARRAY JOIN expands an empty array into a default-valued row, and that predicate sits above
-- the ARRAY JOIN step, so it never reaches the skip index.
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
-- The skip index must be absent from the plan, not merely agree on the count.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_left LEFT ARRAY JOIN tags WHERE tags IN ('')) WHERE explain ILIKE '%Name: idx_tags%';

DROP TABLE t_arrayjoin_bf_left;

-- Hash-domain gate: comparison coerces more widely than the conversion the derivations hash
-- through, so they only fire where the two agree. Every case must match index on/off, and not raise.
DROP TABLE IF EXISTS t_arrayjoin_bf_domain_str;

CREATE TABLE t_arrayjoin_bf_domain_str
(
    id UInt64,
    s Array(String),
    INDEX idx_s s TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Row 2 stores a trailing NUL, which compares equal to the unpadded FixedString constant.
INSERT INTO t_arrayjoin_bf_domain_str VALUES (1, ['abc']), (2, ['abc\0']);

-- String element vs FixedString constant: comparison strips the padding the conversion keeps.
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) = toFixedString('abc', 5) SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) = toFixedString('abc', 5) SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) IN (SELECT toFixedString('abc', 5)) SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) IN (SELECT toFixedString('abc', 5)) SETTINGS use_skip_indexes = 0);
-- String element vs Enum constant: the field carries the number, the element stores the label.
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) = CAST('abc', 'Enum8(\'abc\' = 1)') SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_str WHERE arrayJoin(s) = CAST('abc', 'Enum8(\'abc\' = 1)') SETTINGS use_skip_indexes = 0);

DROP TABLE t_arrayjoin_bf_domain_str;

DROP TABLE IF EXISTS t_arrayjoin_bf_domain_fixed;

CREATE TABLE t_arrayjoin_bf_domain_fixed
(
    id UInt64,
    f Array(FixedString(3)),
    INDEX idx_f f TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_arrayjoin_bf_domain_fixed VALUES (1, ['V0']);

-- A wider FixedString set element: the narrowing conversion would throw TOO_LARGE_STRING_SIZE.
SELECT count() FROM t_arrayjoin_bf_domain_fixed WHERE arrayJoin(f) IN (SELECT toFixedString('V0', 5)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_fixed WHERE arrayJoin(f) IN (SELECT toFixedString('V0', 5)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_domain_fixed;

DROP TABLE IF EXISTS t_arrayjoin_bf_domain_num;

CREATE TABLE t_arrayjoin_bf_domain_num
(
    id UInt64,
    u Array(UInt8),
    f Array(Float64),
    INDEX idx_u u TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_f f TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Negative zero compares equal to positive zero but hashes differently.
INSERT INTO t_arrayjoin_bf_domain_num VALUES (1, [5], [-0.0]), (2, [7], [1.25]);

-- Numeric element vs unparsable String set: the conversion would throw CANNOT_PARSE_TEXT.
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) IN (SELECT 'not-a-number') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) IN (SELECT 'not-a-number') SETTINGS use_skip_indexes = 0;
-- Float element: -0.0 must still be found by an equality against +0.0.
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(f) = 0.0 SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(f) = 0.0 SETTINGS use_skip_indexes = 0);
-- Float element vs Decimal constant: the conversion would throw TYPE_MISMATCH.
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(f) = toDecimal64(1.25, 2) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(f) = toDecimal64(1.25, 2) SETTINGS use_skip_indexes = 0;
-- Cross-integer stays admitted: a UInt8 element against a UInt64 constant still prunes, both forms.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) = toUInt64(5)) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) IN (SELECT toUInt64(5))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) = toUInt64(5) SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) = toUInt64(5) SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) IN (SELECT toUInt64(5)) SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) IN (SELECT toUInt64(5)) SETTINGS use_skip_indexes = 0);
-- A value outside the element's range matches nothing, with or without the index.
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) = 100000 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_num WHERE arrayJoin(u) = 100000 SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_domain_num;

DROP TABLE IF EXISTS t_arrayjoin_bf_domain_ip;

CREATE TABLE t_arrayjoin_bf_domain_ip
(
    id UInt64,
    v Array(IPv4),
    INDEX idx_v v TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_arrayjoin_bf_domain_ip VALUES (1, ['1.2.3.4']);

-- IPv4 element vs the equivalent mapped IPv6 constant: the conversion would throw TYPE_MISMATCH.
SELECT count() FROM t_arrayjoin_bf_domain_ip WHERE arrayJoin(v) = toIPv6('::ffff:1.2.3.4') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_ip WHERE arrayJoin(v) = toIPv6('::ffff:1.2.3.4') SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_domain_ip;

DROP TABLE IF EXISTS t_arrayjoin_bf_domain_enum;

CREATE TABLE t_arrayjoin_bf_domain_enum
(
    id UInt64,
    e Array(Enum8('a' = 1, 'b' = 2)),
    INDEX idx_e e TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Row 2 holds a different label, so a granule can be pruned.
INSERT INTO t_arrayjoin_bf_domain_enum VALUES (1, ['a']), (2, ['b']);

-- An unknown label with validation disabled: the conversion would throw UNKNOWN_ELEMENT_OF_ENUM.
SELECT count() FROM t_arrayjoin_bf_domain_enum WHERE arrayJoin(e) = 'missing' SETTINGS use_skip_indexes = 1, validate_enum_literals_in_operators = 0;
SELECT count() FROM t_arrayjoin_bf_domain_enum WHERE arrayJoin(e) = 'missing' SETTINGS use_skip_indexes = 0, validate_enum_literals_in_operators = 0;
-- The matching label still prunes: an identical element and constant type is admitted.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_domain_enum WHERE arrayJoin(e) = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)')) WHERE explain ILIKE '%Granules: 1/2%';
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_enum WHERE arrayJoin(e) = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') SETTINGS use_skip_indexes = 1);
SELECT groupArray(id) FROM (SELECT id FROM t_arrayjoin_bf_domain_enum WHERE arrayJoin(e) = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') SETTINGS use_skip_indexes = 0);

DROP TABLE t_arrayjoin_bf_domain_enum;

DROP TABLE IF EXISTS t_arrayjoin_bf_domain_lc;

CREATE TABLE t_arrayjoin_bf_domain_lc
(
    id UInt64,
    tags Array(LowCardinality(String)),
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_arrayjoin_bf_domain_lc SELECT number, [concat('tag_', toString(number))] FROM numbers(100000);

-- A LowCardinality wrapper is stripped before the comparison, so both derivations still prune.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_domain_lc WHERE arrayJoin(tags) = 'tag_42') WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arrayjoin_bf_domain_lc WHERE arrayJoin(tags) IN ('tag_42')) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_arrayjoin_bf_domain_lc WHERE arrayJoin(tags) = 'tag_42' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_arrayjoin_bf_domain_lc WHERE arrayJoin(tags) = 'tag_42' SETTINGS use_skip_indexes = 0;

DROP TABLE t_arrayjoin_bf_domain_lc;
