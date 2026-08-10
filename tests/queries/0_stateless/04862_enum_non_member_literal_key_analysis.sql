-- Tags: no-parallel-replicas
-- At validate_enum_literals_in_operators = 0 (the default) a non-member string literal must not
-- throw, and the answer must not depend on whether the Enum column is in a key or carries an index.
-- Every predicate below sits in a WHERE clause: only there does index analysis run, and it is index
-- analysis that used to throw. The same predicate inside countIf() never reaches it.

DROP TABLE IF EXISTS t_key;
DROP TABLE IF EXISTS t_nonkey;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_minmax;
DROP TABLE IF EXISTS t_set;
DROP TABLE IF EXISTS t_bf;
DROP TABLE IF EXISTS t_bf_arr;
DROP TABLE IF EXISTS t_bf_map;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_enum16;

CREATE TABLE t_key    (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY e;
CREATE TABLE t_nonkey (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_key    VALUES ('a'), ('b');
INSERT INTO t_nonkey VALUES ('a'), ('b');

SELECT 'positive control';
SELECT count() FROM t_key;
SELECT count() FROM t_key WHERE e = 'a';
SELECT count() FROM t_nonkey;
SELECT count() FROM t_nonkey WHERE e = 'a';

-- All six comparison operators against a non-member literal, on the key and on the non-key table.
-- The key and non-key answers must be identical: a query's validity and result cannot depend on
-- the physical layout.
SELECT 'six operators vs a non-member literal, key table';
SELECT count() FROM t_key WHERE e =  '4';
SELECT count() FROM t_key WHERE e <  '4';
SELECT count() FROM t_key WHERE e <= '4';
SELECT count() FROM t_key WHERE e >  '4';
SELECT count() FROM t_key WHERE e >= '4';
SELECT count() FROM t_key WHERE e != '4';
SELECT 'six operators vs a non-member literal, non-key table';
SELECT count() FROM t_nonkey WHERE e =  '4';
SELECT count() FROM t_nonkey WHERE e <  '4';
SELECT count() FROM t_nonkey WHERE e <= '4';
SELECT count() FROM t_nonkey WHERE e >  '4';
SELECT count() FROM t_nonkey WHERE e >= '4';
SELECT count() FROM t_nonkey WHERE e != '4';

SELECT 'a literal that is not even numeric-looking';
SELECT count() FROM t_key    WHERE e = 'zzz';
SELECT count() FROM t_key    WHERE e < 'zzz';
SELECT count() FROM t_key    WHERE e != 'zzz';
SELECT count() FROM t_nonkey WHERE e = 'zzz';
SELECT count() FROM t_nonkey WHERE e < 'zzz';
SELECT count() FROM t_nonkey WHERE e != 'zzz';

SELECT 'null-safe operators';
SELECT count() FROM t_key    WHERE e <=> '4';
SELECT count() FROM t_nonkey WHERE e <=> '4';
SELECT count() FROM t_key    WHERE isDistinctFrom(e, '4');
SELECT count() FROM t_nonkey WHERE isDistinctFrom(e, '4');
SELECT count() FROM t_key    WHERE e <=> 'a';
SELECT count() FROM t_key    WHERE isDistinctFrom(e, 'a');

SELECT 'string search over an Enum key';
SELECT count() FROM t_key    WHERE e LIKE '%b%';
SELECT count() FROM t_nonkey WHERE e LIKE '%b%';
SELECT count() FROM t_key    WHERE e NOT LIKE '%b%';
SELECT count() FROM t_nonkey WHERE e NOT LIKE '%b%';
SELECT count() FROM t_key    WHERE match(e, 'b');
SELECT count() FROM t_nonkey WHERE match(e, 'b');
SELECT count() FROM t_key    WHERE e LIKE '%zzz%';
SELECT count() FROM t_nonkey WHERE e LIKE '%zzz%';

SELECT 'partition key';
CREATE TABLE t_part (x Enum8('Alpha' = 1, 'Beta' = 2), v UInt64) ENGINE = MergeTree PARTITION BY x ORDER BY v;
INSERT INTO t_part SELECT if(number % 2 = 0, 'Alpha', 'Beta'), number FROM numbers(1000);
SELECT count() FROM t_part;
SELECT count() FROM t_part WHERE x LIKE '%Beta%';
SELECT count() FROM t_part WHERE x LIKE '%Beta%' SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_part WHERE x = 'Beta';
SELECT count() FROM t_part WHERE x = 'zzz';
SELECT count() FROM t_part WHERE x = 'zzz' SETTINGS use_partition_pruning = 0;

SELECT 'skip indexes on a non-key Enum column';
CREATE TABLE t_minmax (e Enum8('a' = 1, 'b' = 2), v UInt64, INDEX i e TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_set    (e Enum8('a' = 1, 'b' = 2), v UInt64, INDEX i e TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_bf     (e Enum8('a' = 1, 'b' = 2), v UInt64, INDEX i e TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_minmax VALUES ('a', 1), ('b', 2);
INSERT INTO t_set    VALUES ('a', 1), ('b', 2);
INSERT INTO t_bf     VALUES ('a', 1), ('b', 2);

SELECT 'minmax';
SELECT count() FROM t_minmax WHERE e = 'a';
SELECT count() FROM t_minmax WHERE e = '4';
SELECT count() FROM t_minmax WHERE e = '4' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_minmax WHERE e != '4';
SELECT count() FROM t_minmax WHERE e LIKE '%b%';
SELECT 'set';
SELECT count() FROM t_set WHERE e = 'a';
SELECT count() FROM t_set WHERE e = '4';
SELECT count() FROM t_set WHERE e = '4' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set WHERE e != '4';
SELECT count() FROM t_set WHERE e LIKE '%b%';
SELECT 'bloom_filter, scalar equality';
SELECT count() FROM t_bf WHERE e = 'a';
SELECT count() FROM t_bf WHERE e = '4';
SELECT count() FROM t_bf WHERE e = '4' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf WHERE e != '4';
-- IN with a mix of member and non-member elements must still return the matching rows.
SELECT count() FROM t_bf WHERE e IN ('4', 'a');
SELECT count() FROM t_bf WHERE e IN ('4', 'a') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf WHERE e NOT IN ('4', 'a');

SELECT 'bloom_filter over an Array(Enum): has / hasAny / hasAll';
CREATE TABLE t_bf_arr (a Array(Enum8('a' = 1, 'b' = 2)), v UInt64, INDEX i a TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_bf_arr VALUES (['a'], 1), (['b'], 2);
SELECT count() FROM t_bf_arr WHERE has(a, 'a');
SELECT count() FROM t_bf_arr WHERE has(a, '4');
SELECT count() FROM t_bf_arr WHERE has(a, '4') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['4']);
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['4']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['4', 'a']);
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['4', 'a']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_arr WHERE hasAll(a, ['4']);
SELECT count() FROM t_bf_arr WHERE hasAll(a, ['4']) SETTINGS use_skip_indexes = 0;

SELECT 'bloom_filter over Map(Enum, ...) keys';
CREATE TABLE t_bf_map (m Map(Enum8('a' = 1, 'b' = 2), UInt64), v UInt64, INDEX i mapKeys(m) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_bf_map VALUES (map('a', 1), 1), (map('b', 2), 2);
SELECT count() FROM t_bf_map WHERE mapContains(m, 'a');
SELECT count() FROM t_bf_map WHERE mapContains(m, '4');
SELECT count() FROM t_bf_map WHERE mapContains(m, '4') SETTINGS use_skip_indexes = 0;

SELECT 'type wrappers';
-- LowCardinality(Enum) is intentionally absent: DataTypeEnum does not override
-- canBeInsideLowCardinality(), so the type cannot be constructed.
CREATE TABLE t_null   (e Nullable(Enum8('a' = 1, 'b' = 2)))  ENGINE = MergeTree ORDER BY e SETTINGS allow_nullable_key = 1;
CREATE TABLE t_enum16 (e Nullable(Enum16('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY e SETTINGS allow_nullable_key = 1;
INSERT INTO t_null   VALUES ('a'), ('b'), (NULL);
INSERT INTO t_enum16 VALUES ('a'), ('b'), (NULL);
SELECT 'Nullable(Enum8)';
SELECT count() FROM t_null WHERE e = 'a';
SELECT count() FROM t_null WHERE e = '4';
SELECT count() FROM t_null WHERE e != '4';
SELECT count() FROM t_null WHERE e <=> '4';
SELECT count() FROM t_null WHERE isDistinctFrom(e, '4');
SELECT 'Nullable(Enum16)';
SELECT count() FROM t_enum16 WHERE e = 'a';
SELECT count() FROM t_enum16 WHERE e = '4';
SELECT count() FROM t_enum16 WHERE e != '4';
SELECT count() FROM t_enum16 WHERE e <=> '4';
SELECT count() FROM t_enum16 WHERE isDistinctFrom(e, '4');

-- A NULL row stays NULL for the plain operators and follows null-safe semantics for the others.
SELECT e, e = '4', e != '4', e <=> '4', isDistinctFrom(e, '4') FROM t_null ORDER BY e NULLS LAST;

SELECT 'validate_enum_literals_in_operators = 1 still rejects';
SELECT count() FROM t_nonkey WHERE e =   '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_nonkey WHERE e !=  '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_nonkey WHERE e <=> '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_nonkey WHERE isDistinctFrom(e, '4') SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_key WHERE e = '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_bf  WHERE e = '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- force_data_skipping_indices raises INDEX_NOT_USED when a skip index ends up
-- alwaysUnknownOrTrue, so it asserts index usability directly instead of via a result count:
-- a member literal must still USE each index, a non-member literal must DECLINE it. Without
-- these, an over-broad decline (every Enum predicate, not just non-member ones) would leave
-- the result counts above byte-identical.
SELECT 'a member literal still uses each skip index';
SELECT count() FROM t_minmax WHERE e = 'a' SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_set    WHERE e = 'a' SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_bf     WHERE e = 'a' SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_bf_arr WHERE has(a, 'a')      SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['a']) SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_bf_arr WHERE hasAll(a, ['a']) SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_bf_map WHERE mapContains(m, 'a') SETTINGS force_data_skipping_indices = 'i';

SELECT 'a non-member literal declines the index instead of throwing';
-- enable_analyzer = 1 where the predicate is a bare scalar comparison: the old analyzer folds it
-- to a constant false and drops the MergeTree read, so no index analysis runs to assert on.
SELECT count() FROM t_minmax WHERE e = '4' SETTINGS force_data_skipping_indices = 'i', enable_analyzer = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf     WHERE e = '4' SETTINGS force_data_skipping_indices = 'i', enable_analyzer = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf_arr WHERE has(a, '4')      SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf_arr WHERE hasAny(a, ['4']) SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf_arr WHERE hasAll(a, ['4']) SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf_map WHERE mapContains(m, '4') SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
-- set(N) decides usability structurally (MergeTreeIndexConditionSet::isUseless is
-- actions == nullptr), not from a KeyCondition atom, so it stays usable for a non-member
-- literal and evaluates the predicate over the stored set instead. Asserting that it still
-- USES the index is the stronger claim here.
SELECT count() FROM t_set WHERE e = '4' SETTINGS force_data_skipping_indices = 'i';

SELECT 'pruning for representable literals is preserved';
SET explain_query_plan_default = 'legacy';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key WHERE e = 'b') WHERE explain ILIKE '%e in [2, 2]%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_part WHERE x = 'Beta') WHERE explain ILIKE '%x in [2, 2]%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_part WHERE x = 'Beta') WHERE explain ILIKE '%Parts: 1/2%';
-- The non-member literal declines analysis rather than throwing: every part stays in. The pin goes
-- on the outer query because enable_analyzer cannot be changed inside a subquery (INCORRECT_QUERY).
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_part WHERE x = 'zzz') WHERE explain ILIKE '%Parts: 2/2%' SETTINGS enable_analyzer = 1;

DROP TABLE t_key;
DROP TABLE t_nonkey;
DROP TABLE t_part;
DROP TABLE t_minmax;
DROP TABLE t_set;
DROP TABLE t_bf;
DROP TABLE t_bf_arr;
DROP TABLE t_bf_map;
DROP TABLE t_null;
DROP TABLE t_enum16;
