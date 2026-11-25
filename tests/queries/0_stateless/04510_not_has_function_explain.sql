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

-- The empty string is the default value of the key type. Without such a row the pure-NULL
-- assertions below cannot fail: a NULL that decayed into the default would match no row either,
-- so the correct and the decayed answers would coincide.
INSERT INTO test_string_null_set VALUES (''), ('a'), ('b');

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
-- An explicit empty string is a real element and matches its row, which is the answer the pure-NULL
-- forms above must not produce.
SELECT count() FROM test_string_null_set WHERE has([''], k);
SELECT count() FROM test_string_null_set WHERE has([''], k) SETTINGS use_primary_key = 0;

-- Dropping the NULL leaves the surviving element usable for pruning, so the mixed set still reads
-- fewer granules than the three a full scan of this table would.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_string_null_set WHERE has(['a', CAST(NULL, 'Nullable(String)')], k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_string_null_set;

-- Dynamic and Variant carry a NULL through a discriminator instead of a null map, and a String
-- target can hold neither. The empty-string key row is what makes the poisoning observable: a NULL
-- that decayed to '' would enter the set as a real element and prune that row away. The two rows
-- must stay in separate parts for that pruning to be visible, so merges are stopped.
DROP TABLE IF EXISTS test_dynamic_null_set;
CREATE TABLE test_dynamic_null_set (k String) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES test_dynamic_null_set;

INSERT INTO test_dynamic_null_set VALUES ('');
INSERT INTO test_dynamic_null_set VALUES ('a');

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_dynamic_null_set' AND active;

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

-- Dropping the unrepresentable NULL leaves the surviving element usable, so the mixed set still
-- prunes rather than giving up the index entirely.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_dynamic_null_set WHERE has(CAST(['a', NULL], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_dynamic_null_set WHERE has(CAST(['a', NULL], 'Array(Dynamic)'), k);
SELECT count() FROM test_dynamic_null_set WHERE has(CAST(['a', NULL], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;

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

-- A Tuple key passes the Nullable-capability check that stops a bare Array or Map key, so the set
-- index would otherwise reach a cast that refuses the Tuple's Array element. The index is declined
-- instead, which keeps the answers correct for both a NULL-free and a NULL-carrying element.
DROP TABLE IF EXISTS test_tuple_array_key;
CREATE TABLE test_tuple_array_key (t Tuple(Array(String))) ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 1;

INSERT INTO test_tuple_array_key VALUES ((['a'])), ((['b']));

SELECT count() FROM test_tuple_array_key WHERE has([CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_key WHERE has([CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))')], t) SETTINGS use_primary_key = 0;
SELECT count() FROM test_tuple_array_key WHERE has([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_key WHERE has([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t) SETTINGS use_primary_key = 0;
SELECT count() FROM test_tuple_array_key WHERE notHas([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_key WHERE notHas([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t) SETTINGS use_primary_key = 0;
SELECT count() FROM test_tuple_array_key WHERE has([tuple(['a'])], t);

DROP TABLE test_tuple_array_key;

-- The same refusal is reached without any Nullable, by a set element whose numeric type merely
-- differs from the key's. That path errored before this change as well.
DROP TABLE IF EXISTS test_tuple_array_numeric_key;
CREATE TABLE test_tuple_array_numeric_key (t Tuple(Array(UInt8))) ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 1;

INSERT INTO test_tuple_array_numeric_key VALUES (([1])), (([2]));

SELECT count() FROM test_tuple_array_numeric_key WHERE has([CAST(tuple([1]), 'Tuple(Array(UInt64))')], t);
SELECT count() FROM test_tuple_array_numeric_key WHERE has([CAST(tuple([1]), 'Tuple(Array(UInt64))')], t) SETTINGS use_primary_key = 0;
SELECT count() FROM test_tuple_array_numeric_key WHERE has([tuple([toUInt8(1)])], t);

DROP TABLE test_tuple_array_numeric_key;

-- A plain Tuple key keeps its set index: only a target the accurate cast refuses is declined.
DROP TABLE IF EXISTS test_tuple_scalar_key;
CREATE TABLE test_tuple_scalar_key (a String, b String) ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO test_tuple_scalar_key SELECT toString(number), toString(number) FROM numbers(16);

SELECT count() FROM test_tuple_scalar_key WHERE (a, b) IN (('3', '3'));
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM test_tuple_scalar_key WHERE (a, b) IN (('3', '3'))) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_scalar_key WHERE (a, b) IN ((CAST('3', 'Nullable(String)'), '3'));
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM test_tuple_scalar_key WHERE (a, b) IN ((CAST('3', 'Nullable(String)'), '3'))) WHERE explain LIKE '%Granules:%/%';

DROP TABLE test_tuple_scalar_key;

-- The same target reaches the cast a second time when the key is an expression, because the set is
-- first pushed through the key's deterministic transform. That path is declined too.
DROP TABLE IF EXISTS test_tuple_array_expr_key;
CREATE TABLE test_tuple_array_expr_key (t Tuple(Array(String))) ENGINE = MergeTree
ORDER BY tupleElement(t, 1)
SETTINGS index_granularity = 1;

INSERT INTO test_tuple_array_expr_key VALUES ((['a'])), ((['b']));

SELECT count() FROM test_tuple_array_expr_key WHERE has([CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_expr_key WHERE has([CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))')], t) SETTINGS use_primary_key = 0;
SELECT count() FROM test_tuple_array_expr_key WHERE has([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_expr_key WHERE notHas([CAST(tuple([NULL]), 'Tuple(Array(Nullable(String)))')], t);
SELECT count() FROM test_tuple_array_expr_key WHERE has([CAST(['a'], 'Array(Nullable(String))')], tupleElement(t, 1));
SELECT count() FROM test_tuple_array_expr_key WHERE has([tuple(['a'])], t);

DROP TABLE test_tuple_array_expr_key;

-- A scalar key reached through the same transform keeps its index, so declining the container target
-- above does not cost pruning here.
DROP TABLE IF EXISTS test_scalar_expr_key;
CREATE TABLE test_scalar_expr_key (k String) ENGINE = MergeTree
ORDER BY concat(k, 'x')
SETTINGS index_granularity = 1;

INSERT INTO test_scalar_expr_key SELECT toString(number) FROM numbers(16);

SELECT count() FROM test_scalar_expr_key WHERE has([CAST('3', 'Nullable(String)')], k);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_scalar_expr_key WHERE has([CAST('3', 'Nullable(String)')], k)) WHERE explain LIKE '%Granules:%/%';

DROP TABLE test_scalar_expr_key;

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

-- A Nullable key can represent the NULL that a Dynamic or a LowCardinality dictionary carries, so
-- those elements keep the set index instead of giving it up the way a non-Nullable key has to.
DROP TABLE IF EXISTS test_nullable_key_prune;
CREATE TABLE test_nullable_key_prune (k Nullable(String)) ENGINE = MergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO test_nullable_key_prune SELECT toString(number) FROM numbers(16);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_prune WHERE has(CAST(['3'], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_nullable_key_prune WHERE has(CAST(['3'], 'Array(Dynamic)'), k);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_prune WHERE has([CAST('3', 'LowCardinality(Nullable(String))')], k)) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_nullable_key_prune WHERE has([CAST('3', 'LowCardinality(Nullable(String))')], k);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_prune WHERE has([CAST('3', 'Nullable(String)')], k)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_prune WHERE has(['3'], k)) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM test_nullable_key_prune WHERE has([CAST(NULL, 'Dynamic')], k);
SELECT count() FROM test_nullable_key_prune WHERE has([CAST(NULL, 'Dynamic')], k) SETTINGS use_primary_key = 0;

DROP TABLE test_nullable_key_prune;

-- The NULL a Dynamic or a LowCardinality dictionary carries must reach a Nullable key as a NULL, so
-- it matches the NULL row and not the empty-string one. isNull reports which row matched, which a
-- count cannot: a NULL that decayed to '' would return the same count off the other row. The two
-- rows stay in separate parts so the granule count also reacts, hence merges are stopped.
DROP TABLE IF EXISTS test_nullable_key_null_elem;
DROP TABLE IF EXISTS test_nullable_key_null_elem_mem;
CREATE TABLE test_nullable_key_null_elem (k Nullable(String)) ENGINE = MergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;
CREATE TABLE test_nullable_key_null_elem_mem (k Nullable(String)) ENGINE = Memory;

SYSTEM STOP MERGES test_nullable_key_null_elem;

INSERT INTO test_nullable_key_null_elem VALUES ('');
INSERT INTO test_nullable_key_null_elem VALUES (NULL);
INSERT INTO test_nullable_key_null_elem_mem VALUES (''), (NULL);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_key_null_elem' AND active;

SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'Dynamic')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'Dynamic')], k) SETTINGS use_primary_key = 0;
SELECT isNull(k) FROM test_nullable_key_null_elem_mem WHERE has([CAST(NULL, 'Dynamic')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE notHas([CAST(NULL, 'Dynamic')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE notHas([CAST(NULL, 'Dynamic')], k) SETTINGS use_primary_key = 0;
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'LowCardinality(Nullable(String))')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'LowCardinality(Nullable(String))')], k) SETTINGS use_primary_key = 0;
SELECT isNull(k) FROM test_nullable_key_null_elem_mem WHERE has([CAST(NULL, 'LowCardinality(Nullable(String))')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE notHas([CAST(NULL, 'LowCardinality(Nullable(String))')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'Nullable(String)')], k);
-- The empty-string row is matchable, which is what makes a decayed NULL observable above, and it is
-- also the arm that proves these assertions can print the other value.
SELECT isNull(k) FROM test_nullable_key_null_elem WHERE has([CAST('', 'Dynamic')], k);
SELECT isNull(k) FROM test_nullable_key_null_elem_mem WHERE has([CAST('', 'Dynamic')], k);
-- The NULL element still prunes on a Nullable key, so it reaches one granule and not both.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'Dynamic')], k)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_null_elem WHERE has([CAST(NULL, 'LowCardinality(Nullable(String))')], k)) WHERE explain LIKE '%Granules:%/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM test_nullable_key_null_elem WHERE has([CAST('', 'Dynamic')], k)) WHERE explain LIKE '%Granules:%/%';

DROP TABLE test_nullable_key_null_elem;
DROP TABLE test_nullable_key_null_elem_mem;

-- A monotonic wrapper whose argument type is the container key itself resolves the constant against
-- that container type, which reaches the same rule.
DROP TABLE IF EXISTS test_mono_container_key;
CREATE TABLE test_mono_container_key (t Tuple(Array(String))) ENGINE = MergeTree
ORDER BY materialize(t)
SETTINGS index_granularity = 1;

INSERT INTO test_mono_container_key VALUES ((['a'])), ((['b']));

SELECT count() FROM test_mono_container_key WHERE t = CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))');
SELECT count() FROM test_mono_container_key WHERE t = CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))') SETTINGS use_primary_key = 0;
SELECT count() FROM test_mono_container_key WHERE t = tuple(['a']);

DROP TABLE test_mono_container_key;

-- A String element does not equal an Enum key under `has`, even though the accurate cast maps it to
-- the label of the same name. Do not turn this into a primary-key set atom, or the granule holding
-- the row with that label would be pruned away under `notHas`.
DROP TABLE IF EXISTS test_not_has_enum;
CREATE TABLE test_not_has_enum (x Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4, 'e' = 5, 'f' = 6, 'g' = 7, 'h' = 8))
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has_enum SELECT toInt8(number + 1) FROM numbers(8);

SELECT count() FROM test_not_has_enum WHERE has(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE has(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SELECT count() FROM test_not_has_enum WHERE notHas(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE notHas(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SELECT count() FROM test_not_has_enum WHERE NOT has(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE NOT has(['c'], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SET optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT x FROM test_not_has_enum WHERE has(['c'], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

-- A numeric element, on the other hand, is compared numerically by `has` just like the set index
-- compares it (see `00674_has_array_enum`), so an Enum key keeps its exact set atom and still prunes.
SELECT count() FROM test_not_has_enum WHERE has([3], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE has([3], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SELECT count() FROM test_not_has_enum WHERE notHas([3], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE notHas([3], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT x FROM test_not_has_enum WHERE has([3], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SET optimize_rewrite_has_to_in = 1;

-- A code with no declared label matches nothing, and the accurate cast drops it from the set, so
-- both forms stay correct.
SELECT count() FROM test_not_has_enum WHERE has([100], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE has([100], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;
SELECT count() FROM test_not_has_enum WHERE notHas([100], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_enum WHERE notHas([100], x) SETTINGS optimize_rewrite_has_to_in = 0, use_primary_key = 0;

DROP TABLE test_not_has_enum;
