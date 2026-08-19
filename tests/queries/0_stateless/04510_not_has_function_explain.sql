-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET explain_query_plan_default = 'legacy';

-- { echo }

DROP TABLE IF EXISTS test_not_has;
CREATE TABLE test_not_has (x UInt64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has SELECT intDiv(number, 4) FROM numbers(24);

-- Without the analyzer rewrite, `notHas` prunes through its own key condition atom, which is the
-- complement of the `has` atom.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- `NOT has` is pushed down to the `notHas` leaf during index analysis, and `NOT notHas` folds back
-- to `has`, so both prune without the analyzer rewrite.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE NOT has([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE NOT has([1], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE NOT notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE NOT notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- When the analyzer rewrite is blocked (NULL array element), the key condition atom still applies:
-- a NULL element can never match the non-Nullable key column, so it is dropped from the set.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE notHas([1, NULL], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x);
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has WHERE NOT has([1, NULL], x);
SELECT count() FROM test_not_has WHERE NOT has([1, NULL], x) SETTINGS use_primary_key = 0;

DROP TABLE test_not_has;

-- The same NULL-element rule applies to `NOT IN` when a subquery set contains a NULL and
-- `transform_null_in` is enabled: the NULL must be dropped from the set, not decayed to the
-- nested type's default value, which would poison the set and prune granules with matching rows.
DROP TABLE IF EXISTS test_null_set;
CREATE TABLE test_null_set (x UInt64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_null_set SELECT intDiv(number, 4) FROM numbers(24);

SET transform_null_in = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL]))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL]));
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL])) SETTINGS use_primary_key = 0;

-- The positive form keeps only the matching rows.
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin([1, NULL]));
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin([1, NULL])) SETTINGS use_primary_key = 0;

-- A set of only NULLs becomes empty: nothing matches the positive form, everything matches the
-- negated form. The set must be typed: an untyped `[NULL]` produces a set of `Nothing`, which the
-- `IN` execution itself cannot compare against the column.
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));

DROP TABLE test_null_set;

-- A NULL set element must be preserved for a Nullable key. It matches the NULL key row, so the
-- negated form can skip that one-row granule.
DROP TABLE IF EXISTS test_nullable_null_set;
CREATE TABLE test_nullable_null_set (x Nullable(UInt64)) ENGINE = MergeTree
ORDER BY x
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_nullable_null_set VALUES (NULL), (1), (2);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x);
SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_nullable_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));
SELECT count() FROM test_nullable_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_primary_key = 0;

DROP TABLE test_nullable_null_set;

-- A NULL set element against a String key must be dropped from the set like it is for any other key
-- type, not rejected. A Nullable source cannot be safely cast to a non-Nullable target, so the set
-- columns take the accurate-or-null conversion that filters unrepresentable elements out.
DROP TABLE IF EXISTS test_string_null_set;
CREATE TABLE test_string_null_set (k String) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO test_string_null_set VALUES ('a'), ('b');

SELECT count() FROM test_string_null_set WHERE has([CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_string_null_set WHERE has([CAST(NULL, 'Nullable(String)')], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE NOT has([CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_string_null_set WHERE notHas([CAST(NULL, 'Nullable(String)')], k);
-- An untyped NULL element arrives as `Nothing` rather than `Nullable`, so it reaches the rule
-- through a different conversion than the CAST forms above.
SELECT count() FROM test_string_null_set WHERE has([NULL], k);
SELECT count() FROM test_string_null_set WHERE has([NULL], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE notHas([NULL], k);
SELECT count() FROM test_string_null_set WHERE notHas([NULL], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE NOT has([NULL], k);
SELECT count() FROM test_string_null_set WHERE NOT has([NULL], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE has(['a', NULL], k);
SELECT count() FROM test_string_null_set WHERE has(['a', NULL], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE has(['a', CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_string_null_set WHERE has(['a', CAST(NULL, 'Nullable(String)')], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_string_null_set WHERE has(['z', CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_string_null_set WHERE has([CAST(NULL, 'LowCardinality(Nullable(String))')], k);
SELECT count() FROM test_string_null_set WHERE has([CAST('a', 'Nullable(String)')], k);
SELECT count() FROM test_string_null_set WHERE has(['a'], k);

-- Dropping the NULL leaves the surviving element usable for pruning, so the mixed set still selects
-- one granule instead of falling back to a full scan.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_string_null_set WHERE has(['a', CAST(NULL, 'Nullable(String)')], k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_string_null_set;

-- Dynamic and Variant carry a NULL through a discriminator instead of a null map, and a String
-- target can hold neither. The empty-string key row is what makes the poisoning observable: a NULL
-- that decayed to '' would enter the set as a real element and prune that row away.
DROP TABLE IF EXISTS test_dynamic_null_set;
CREATE TABLE test_dynamic_null_set (k String) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO test_dynamic_null_set VALUES ('');
INSERT INTO test_dynamic_null_set VALUES ('a');

SELECT count() FROM test_dynamic_null_set WHERE notHas(CAST([NULL], 'Array(Dynamic)'), k);
SELECT count() FROM test_dynamic_null_set WHERE notHas(CAST([NULL], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_dynamic_null_set WHERE has(CAST([NULL], 'Array(Dynamic)'), k);
SELECT count() FROM test_dynamic_null_set WHERE has(CAST([NULL], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_dynamic_null_set WHERE notHas(CAST(['a', NULL], 'Array(Dynamic)'), k);
SELECT count() FROM test_dynamic_null_set WHERE notHas(CAST(['a', NULL], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;

SET transform_null_in = 1;
SELECT count() FROM test_dynamic_null_set WHERE k NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Variant(String, UInt8))')));
SELECT count() FROM test_dynamic_null_set WHERE k NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Variant(String, UInt8))'))) SETTINGS use_primary_key = 0;
SELECT count() FROM test_dynamic_null_set WHERE k IN (SELECT arrayJoin(CAST([NULL], 'Array(Variant(String, UInt8))')));
SELECT count() FROM test_dynamic_null_set WHERE k IN (SELECT arrayJoin(CAST([NULL], 'Array(Variant(String, UInt8))'))) SETTINGS use_primary_key = 0;
SET transform_null_in = 0;

SELECT count() FROM test_dynamic_null_set WHERE has(CAST(['a'], 'Array(Dynamic)'), k);

DROP TABLE test_dynamic_null_set;

-- A NULL-free Dynamic element keeps its set index, so only the unrepresentable NULL loses the fast
-- path. Selecting the key rather than count() keeps the read off the exact-count projection.
DROP TABLE IF EXISTS test_dynamic_prune;
CREATE TABLE test_dynamic_prune (k String) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO test_dynamic_prune SELECT toString(number) FROM numbers(16);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_dynamic_prune WHERE has(CAST(['3'], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_dynamic_prune WHERE has(['3'], k)) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_dynamic_prune WHERE has(CAST(['3'], 'Array(Dynamic)'), k);

DROP TABLE test_dynamic_prune;

-- Array and Map elements reach the same rule by recursion on their nested types.
DROP TABLE IF EXISTS test_array_null_set;
CREATE TABLE test_array_null_set (a Array(String)) ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1;

INSERT INTO test_array_null_set SELECT [toString(number)] FROM numbers(16);

SELECT count() FROM test_array_null_set WHERE has([CAST([NULL], 'Array(Nullable(String))')], a);
SELECT count() FROM test_array_null_set WHERE has([['3', NULL]], a);
SELECT count() FROM test_array_null_set WHERE has([['3']], a);

-- An Array or Map key cannot be inside Nullable, so a Nullable-nested element gives up the set
-- index even when it holds no NULL: the plain element below prunes, this one scans. Results stay
-- correct either way, so only the pruning is traded for the correctness fix above.
SELECT count() FROM test_array_null_set WHERE has([CAST(['3'], 'Array(Nullable(String))')], a);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_array_null_set WHERE has([CAST(['3'], 'Array(Nullable(String))')], a)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_array_null_set WHERE has([['3']], a)) WHERE explain LIKE '%Granules:%/%';

DROP TABLE test_array_null_set;

DROP TABLE IF EXISTS test_map_null_set;
CREATE TABLE test_map_null_set (m Map(String, String)) ENGINE = MergeTree
ORDER BY m
SETTINGS index_granularity = 1;

INSERT INTO test_map_null_set SELECT map('k', toString(number)) FROM numbers(16);

SELECT count() FROM test_map_null_set WHERE has([map('k', NULL)], m);
SELECT count() FROM test_map_null_set WHERE has([map('k', '3')], m);
SELECT count() FROM test_map_null_set WHERE has([CAST(map('k', '3'), 'Map(String, Nullable(String))')], m);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_map_null_set WHERE has([CAST(map('k', '3'), 'Map(String, Nullable(String))')], m)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_map_null_set WHERE has([map('k', '3')], m)) WHERE explain LIKE '%Granules:%/%';

DROP TABLE test_map_null_set;

-- A key built from a monotonic function chain resolves the set against the chain's result type,
-- which reaches the same rule.
DROP TABLE IF EXISTS test_chain_null_set;
CREATE TABLE test_chain_null_set (k String) ENGINE = MergeTree
ORDER BY lower(k)
SETTINGS index_granularity = 1;

INSERT INTO test_chain_null_set VALUES ('A'), ('B');

SELECT count() FROM test_chain_null_set WHERE has([CAST(NULL, 'Nullable(String)')], lower(k));
SELECT count() FROM test_chain_null_set WHERE has([CAST('a', 'Nullable(String)')], lower(k));

DROP TABLE test_chain_null_set;

-- A Nullable String key keeps the NULL element, because the target can represent it: the element
-- matches the NULL row instead of being filtered out.
DROP TABLE IF EXISTS test_nullable_string_key;
CREATE TABLE test_nullable_string_key (k Nullable(String)) ENGINE = MergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO test_nullable_string_key VALUES (NULL), ('a');

SELECT count() FROM test_nullable_string_key WHERE has([CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_nullable_string_key WHERE has([CAST(NULL, 'Nullable(String)')], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_nullable_string_key WHERE notHas([CAST(NULL, 'Nullable(String)')], k);
SELECT count() FROM test_nullable_string_key WHERE has([CAST('a', 'Nullable(String)')], k);

DROP TABLE test_nullable_string_key;
