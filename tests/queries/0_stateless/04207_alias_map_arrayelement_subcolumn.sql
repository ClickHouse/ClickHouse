-- Verify that FunctionToSubcolumnsPass looks through trivial ALIAS columns down
-- to the underlying storage column. A trivial ALIAS body is a single ColumnNode
-- (possibly chained through more trivial ALIAS columns); non-trivial bodies
-- (function calls, casts, ARRAY JOIN expressions) must keep the original
-- bail-out so the rewrite is not applied to a synthetic value.
--
-- Without the look-through, m_alias['k'] reads the entire Map while m['k'] reads
-- only the matching key subcolumn, so the alias path was orders of magnitude slower.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_alias_map;

-- The ALIAS chain mixes orderings: a4 forward-references a3 (which is defined later).
-- a_bad is a non-trivial alias; a_after_bad is a trivial alias whose body is a_bad
-- (itself non-trivial), so the chain must bail out at the non-trivial step.
CREATE TABLE t_alias_map
(
    id UInt64,
    m Map(String, UInt64),

    a1 ALIAS m,
    a2 ALIAS a1,
    a3 ALIAS a2,
    a4 ALIAS a3,

    a_fwd ALIAS a_fwd_inner,
    a_fwd_inner ALIAS m,

    a_bad ALIAS mapApply((k, v) -> (k, v + 1), m),
    a_after_bad ALIAS a_bad
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_map (id, m)
    SELECT number, map('key1', number, 'key2', number + 1) FROM numbers(10);

-- ==========================================
-- Section 1: Baseline. Direct column rewrites to the key subcolumn.
-- ==========================================

SELECT '-- baseline: m[key1] rewrites to m.key_key1';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE m['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 2: 1-level trivial ALIAS (a1 ALIAS m).
-- ==========================================

SELECT '-- 1-level alias: a1[key1] rewrites to m.key_key1 (in WHERE)';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a1['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a1['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';

SELECT '-- 1-level alias: a1[key1] rewrites to m.key_key1 (in SELECT)';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT a1['key1'] FROM t_alias_map) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT a1['key1'] FROM t_alias_map) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 3: 2-level chain (a2 ALIAS a1 ALIAS m).
-- ==========================================

SELECT '-- 2-level chain: a2[key1] rewrites to m.key_key1';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a2['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a2['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 4: Deep chain (a4 ALIAS a3 ALIAS a2 ALIAS a1 ALIAS m).
-- The walk must follow every step.
-- ==========================================

SELECT '-- 4-level chain: a4[key1] rewrites to m.key_key1';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a4['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a4['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 5: Forward-defined chain (a_fwd ALIAS a_fwd_inner, a_fwd_inner ALIAS m).
-- Definition order should not matter; the analyzer resolves both.
-- ==========================================

SELECT '-- forward-defined chain: a_fwd[key1] rewrites to m.key_key1';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_fwd['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_fwd['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 6: Non-trivial ALIAS body (a_bad ALIAS mapApply(...)).
-- Must NOT be rewritten: the alias body produces a different Map.
-- ==========================================

SELECT '-- non-trivial alias: a_bad[key1] is NOT rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_bad['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_bad['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 7: Chain that ends in a non-trivial alias (a_after_bad ALIAS a_bad ALIAS mapApply(...)).
-- The walk must stop at the non-trivial step and skip the optimization.
-- ==========================================

SELECT '-- chain through non-trivial alias: a_after_bad[key1] is NOT rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_after_bad['key1'] > 5) WHERE explain LIKE '%FUNCTION arrayElement%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a_after_bad['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 8: Non-constant key on an alias must NOT be rewritten.
-- ==========================================

SELECT '-- non-constant key on alias: a1[k] keeps arrayElement';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, a1[toString(id)] FROM t_alias_map) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 9: optimize_functions_to_subcolumns = 0. Alias path must also respect the setting.
-- ==========================================

SELECT '-- setting off: a1[key1] is NOT rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map WHERE a1['key1'] > 5 SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%FUNCTION arrayElement%';

-- ==========================================
-- Section 10: Filter-only optimization. Alias used both in SELECT (full column) and in WHERE.
-- The Map arrayElement transformer is in transformers_optimize_in_filter_with_full_column,
-- so the WHERE side should still rewrite even when the SELECT reads the whole alias.
-- ==========================================

SELECT '-- filter_only: a1 in SELECT + a1[key1] in WHERE, WHERE side rewrites';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT a1, id FROM t_alias_map WHERE a1['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 11: Other Map transformers via alias chain (length, empty, notEmpty, mapKeys, mapContainsKey).
-- ==========================================

SELECT '-- length(a4) rewrites to m.size0';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT length(a4) FROM t_alias_map) WHERE explain LIKE '%m.size0%';

SELECT '-- empty(a2) rewrites using m.size0';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT empty(a2) FROM t_alias_map) WHERE explain LIKE '%m.size0%';

SELECT '-- mapKeys(a1) rewrites to m.keys';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapKeys(a1) FROM t_alias_map) WHERE explain LIKE '%m.keys%';

SELECT '-- mapContainsKey(a3, key1) rewrites to has(m.keys, key1)';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapContainsKey(a3, 'key1') FROM t_alias_map) WHERE explain LIKE '%m.keys%';

DROP TABLE t_alias_map;

-- ==========================================
-- Section 12: User's exact case with Map(LowCardinality(String), String).
-- ==========================================

DROP TABLE IF EXISTS t_demo;
CREATE TABLE t_demo
(
    attributes Map(LowCardinality(String), String),
    some_alias ALIAS attributes,
    chained_alias ALIAS some_alias
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_demo (attributes) VALUES
    (map('key1810', 'value-a', 'other', 'value-b')),
    (map('key1810', 'value-c'));

SELECT '-- LowCardinality key: some_alias[key1810] rewrites to attributes.key_key1810';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT some_alias['key1810'] FROM t_demo) WHERE explain LIKE '%attributes.key_key1810%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT some_alias['key1810'] FROM t_demo) WHERE explain LIKE '%FUNCTION arrayElement%';

SELECT '-- LowCardinality key: chained_alias also rewrites';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT chained_alias['key1810'] FROM t_demo) WHERE explain LIKE '%attributes.key_key1810%';

SELECT '-- LowCardinality key: same results through every reference';
SELECT attributes['key1810'], some_alias['key1810'], chained_alias['key1810'] FROM t_demo ORDER BY attributes['key1810'];

DROP TABLE t_demo;

-- ==========================================
-- Section 13: Map column in primary key. The index-safe path must still apply through aliases.
-- ==========================================

DROP TABLE IF EXISTS t_alias_map_pk;
CREATE TABLE t_alias_map_pk
(
    id UInt64,
    m Map(String, UInt64),
    a ALIAS m
)
ENGINE = MergeTree ORDER BY (id, m);

INSERT INTO t_alias_map_pk (id, m) SELECT number, map('key1', number) FROM numbers(10);

SELECT '-- alias on PK Map column: a[key1] still rewrites';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_alias_map_pk WHERE a['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

DROP TABLE t_alias_map_pk;

-- ==========================================
-- Section 14: Tuple alias for tupleElement.
-- ==========================================

DROP TABLE IF EXISTS t_alias_tuple;
CREATE TABLE t_alias_tuple
(
    id UInt64,
    t Tuple(a UInt64, b String),
    t_alias ALIAS t,
    t_chain ALIAS t_alias
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_tuple (id, t) VALUES (1, (10, 'x')), (2, (20, 'y'));

-- Use explicit tupleElement() so the function pass (not the resolver/getSubcolumn path) handles it.
SELECT '-- tupleElement(t_alias, a) rewrites to t.a';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT tupleElement(t_alias, 'a') FROM t_alias_tuple) WHERE explain LIKE '%t.a%';

SELECT '-- tupleElement(t_chain, b) rewrites to t.b through chain';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT tupleElement(t_chain, 'b') FROM t_alias_tuple) WHERE explain LIKE '%t.b%';

DROP TABLE t_alias_tuple;

-- ==========================================
-- Section 15: Nullable alias for isNull / isNotNull / count.
-- ==========================================

DROP TABLE IF EXISTS t_alias_nullable;
CREATE TABLE t_alias_nullable
(
    id UInt64,
    n Nullable(String),
    n_alias ALIAS n,
    n_chain ALIAS n_alias
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_nullable (id, n) VALUES (1, 'a'), (2, NULL), (3, 'b');

SELECT '-- isNull(n_alias) rewrites to n.null';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT isNull(n_alias) FROM t_alias_nullable) WHERE explain LIKE '%n.null%';

SELECT '-- isNotNull(n_chain) rewrites to not(n.null) through chain';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT isNotNull(n_chain) FROM t_alias_nullable) WHERE explain LIKE '%n.null%';

SELECT '-- count(n_alias) rewrites to sum(not(n.null))';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count(n_alias) FROM t_alias_nullable) WHERE explain LIKE '%n.null%';

DROP TABLE t_alias_nullable;

-- ==========================================
-- Section 16: Array alias for length / empty.
-- ==========================================

DROP TABLE IF EXISTS t_alias_array;
CREATE TABLE t_alias_array
(
    id UInt64,
    a Array(UInt64),
    a_alias ALIAS a,
    a_chain ALIAS a_alias
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_array (id, a) VALUES (1, [1, 2, 3]), (2, []);

SELECT '-- length(a_alias) rewrites to a.size0';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT length(a_alias) FROM t_alias_array) WHERE explain LIKE '%a.size0%';

SELECT '-- empty(a_chain) uses a.size0 through chain';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT empty(a_chain) FROM t_alias_array) WHERE explain LIKE '%a.size0%';

DROP TABLE t_alias_array;

-- ==========================================
-- Section 17: Correctness sweep. Every reference of the same logical value
-- must produce identical results, regardless of the optimization setting.
-- ==========================================

DROP TABLE IF EXISTS t_alias_correctness;
CREATE TABLE t_alias_correctness
(
    id UInt64,
    m Map(String, UInt64),
    a1 ALIAS m,
    a2 ALIAS a1,
    a3 ALIAS a2,
    a_bad ALIAS mapApply((k, v) -> (k, v + 1), m)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_correctness (id, m)
    SELECT number, map('key1', number, 'key2', number * 10) FROM numbers(5);

SELECT '-- correctness: m, a1, a2, a3 produce identical key1 lookups (with optimization)';
SELECT id, m['key1'], a1['key1'], a2['key1'], a3['key1'], a_bad['key1']
FROM t_alias_correctness
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '-- correctness: identical results without the optimization';
SELECT id, m['key1'], a1['key1'], a2['key1'], a3['key1'], a_bad['key1']
FROM t_alias_correctness
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- correctness: missing key returns default 0 through every alias';
SELECT id, m['nope'], a1['nope'], a2['nope'], a3['nope']
FROM t_alias_correctness
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_alias_correctness;

-- ==========================================
-- Section 18: Defensive / negative cases.
-- The look-through must NOT fire when the ColumnNode's expression points at a
-- column of a different source (ARRAY JOIN, JOIN USING, subquery). Substituting
-- those would change query semantics. Each case asserts both correctness and
-- that no spurious key-subcolumn read appears in the plan.
-- ==========================================

-- ----- ARRAY JOIN -----
-- The unrolled `m` is a Map(...), and its expression is the Array(Map(...)) source column
-- on the table. Different types and different node types, so the rewrite must NOT fire.
DROP TABLE IF EXISTS t_array_join;
CREATE TABLE t_array_join
(
    id UInt64,
    arr Array(Map(String, UInt64))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_array_join VALUES
    (1, [map('key1', 10, 'key2', 20), map('key1', 11)]),
    (2, [map('key1', 30)]);

SELECT '-- ARRAY JOIN: m[key1] on the unrolled element returns the per-row value';
SELECT id, m['key1'] FROM t_array_join ARRAY JOIN arr AS m ORDER BY id, m['key1'];

SELECT '-- ARRAY JOIN: no spurious arr.key_key1 subcolumn appears in the plan';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1 SELECT id, m['key1'] FROM t_array_join ARRAY JOIN arr AS m
) WHERE explain LIKE '%arr.key_key1%';

DROP TABLE t_array_join;

-- ----- JOIN USING -----
-- The USING'd column has source = JoinNode, and its expression is a ListNode containing
-- both sides. The walk must stop at the ListNode and not try to rewrite the join'd Map.
DROP TABLE IF EXISTS t_using_left;
DROP TABLE IF EXISTS t_using_right;
CREATE TABLE t_using_left  (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_using_right (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_using_left  VALUES (1, map('key1', 10)), (2, map('key1', 20));
INSERT INTO t_using_right VALUES (1, map('key1', 10)), (2, map('key1', 20));

SELECT '-- JOIN USING: m[key1] on the USING column returns the per-row value';
SELECT id, m['key1'] FROM t_using_left JOIN t_using_right USING (id, m) ORDER BY id;

SELECT '-- JOIN USING: FUNCTION arrayElement is computed (rewrite did not silently fire)';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT id, m['key1'] FROM t_using_left JOIN t_using_right USING (id, m)
) WHERE explain LIKE '%FUNCTION arrayElement%';

DROP TABLE t_using_left;
DROP TABLE t_using_right;

-- ----- Subquery alias passed to outer arrayElement -----
-- Outer reference's source is the subquery (QueryNode), not a TableNode. Even if the
-- subquery aliases a Map column under a new name, the outer rewrite must not fire.
DROP TABLE IF EXISTS t_subquery_inner;
CREATE TABLE t_subquery_inner
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_subquery_inner VALUES
    (1, map('key1', 100, 'key2', 200)),
    (2, map('key1', 300));

SELECT '-- Subquery: outer m[key1] reads through the subquery and returns the right value';
SELECT id, m['key1'] FROM (SELECT id, m FROM t_subquery_inner) ORDER BY id;

SELECT '-- Subquery: no spurious m.key_key1 ColumnNode is built for the outer reference';
-- Inside the subquery the rewrite is allowed (storage column there); outside, the
-- ColumnNode's source is the subquery's QueryNode and the look-through must bail out.
-- We only assert the value-correctness above and the absence of the rewrite for the
-- outer scope by inspecting the renamed alias `outer_m`.
SELECT count() = 0 FROM (
    EXPLAIN actions = 1 SELECT outer_m['key1'] FROM (SELECT m AS outer_m FROM t_subquery_inner)
) WHERE explain LIKE '%outer_m.key_key1%';

DROP TABLE t_subquery_inner;
