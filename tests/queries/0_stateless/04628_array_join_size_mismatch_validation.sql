-- Regression test for issue #111747: count()/projection over a multi-array ARRAY JOIN with
-- mismatched per-row array sizes must throw SIZES_OF_ARRAYS_DONT_MATCH, exactly like SELECT *.
-- Previously the unused sibling arrays were pruned before execution, so the size check was skipped
-- and the query silently returned a result that matched no valid materialization.

-- The size check is enforced by the analyzer pass in aligned mode only.
SET enable_analyzer = 1;
SET enable_unaligned_array_join = 0;
-- Randomized in CI. An unused operand is replaced by an expression built from its lengths, and
-- whether that expression reads a size subcolumn is decided by FunctionToSubcolumnsPass, so both
-- values must give the same verdicts. The mismatch cases are repeated with 0 at the end.
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_mismatch;
CREATE TABLE t_mismatch (id UInt32, a Array(String), b Array(String), c Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mismatch VALUES (1, ['x', 'y'], ['p'], ['m', 'n']);

SELECT 'Mismatched sizes: every projection must throw, matching SELECT *';
SELECT count() FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT a FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_mismatch ARRAY JOIN a, b, c; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT c FROM t_mismatch ARRAY JOIN a, b, c; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_mismatch LEFT ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT * FROM t_mismatch ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_mismatch;

DROP TABLE IF EXISTS t_match;
CREATE TABLE t_match (id UInt32, a Array(String), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_match VALUES (1, ['x', 'y'], ['p', 'q']), (2, ['z'], ['r']);

SELECT 'Matching sizes: count() and projection are unchanged';
SELECT count() FROM t_match ARRAY JOIN a, b;
SELECT a FROM t_match ARRAY JOIN a, b ORDER BY a;
SELECT count() FROM t_match LEFT ARRAY JOIN a, b;

DROP TABLE t_match;

DROP TABLE IF EXISTS t_single;
CREATE TABLE t_single (id UInt32, a Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_single VALUES (1, ['x', 'y', 'z']);

SELECT 'Single-array ARRAY JOIN is unaffected';
SELECT count() FROM t_single ARRAY JOIN a;
SELECT a FROM t_single ARRAY JOIN a ORDER BY a;

DROP TABLE t_single;

-- A Map operand contributes its size the same way an Array does.
DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (id UInt32, m Map(String, UInt32), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_map VALUES (1, {'x': 1, 'y': 2}, ['p']);

SELECT 'Map operand';
SELECT count() FROM t_map ARRAY JOIN m, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

TRUNCATE TABLE t_map;
INSERT INTO t_map VALUES (1, {'x': 1}, ['p']);
SELECT count() FROM t_map ARRAY JOIN m, b;
-- Only the Map's size is read, not the Map itself. The last row is the live control for the
-- Map(String, UInt32) pattern: there m IS read in full, so a never-matching pattern would fail it.
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_map ARRAY JOIN m, b)
    WHERE explain ILIKE '%m.size0 UInt64%' SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM (EXPLAIN header = 1 SELECT count() FROM t_map ARRAY JOIN m, b)
    WHERE explain ILIKE '%Map(String, UInt32)%' SETTINGS enable_parallel_replicas = 0;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT m FROM t_map ARRAY JOIN m, b)
    WHERE explain ILIKE '%Map(String, UInt32)%' SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_map;

-- Empty arrays: LEFT keeps the row, plain drops it, unchanged by the substitution. The unused-operand
-- spelling must agree with the spelling where both operands are used.
DROP TABLE IF EXISTS t_empty;
CREATE TABLE t_empty (id UInt32, a Array(String), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_empty VALUES (1, [], []), (2, ['x'], ['p']);

SELECT 'Empty arrays';
-- a is unused, so it is substituted; the second spelling uses both operands and is the control.
SELECT count() FROM t_empty LEFT ARRAY JOIN a, b;
SELECT count() FROM (SELECT a, b FROM t_empty LEFT ARRAY JOIN a, b);
SELECT count() FROM t_empty ARRAY JOIN a, b;
SELECT count() FROM (SELECT a, b FROM t_empty ARRAY JOIN a, b);

TRUNCATE TABLE t_empty;
INSERT INTO t_empty VALUES (1, [], []);
SELECT count() FROM t_empty LEFT ARRAY JOIN a, b;
SELECT count() FROM (SELECT a, b FROM t_empty LEFT ARRAY JOIN a, b);
SELECT count() FROM t_empty ARRAY JOIN a, b;
SELECT count() FROM (SELECT a, b FROM t_empty ARRAY JOIN a, b);

DROP TABLE t_empty;

-- Element type wrappers must all reach the check.
DROP TABLE IF EXISTS t_types;
CREATE TABLE t_types
(
    id UInt32,
    lc Array(LowCardinality(String)),
    nl Array(Nullable(Int64)),
    aa Array(Array(Int64)),
    tp Array(Tuple(x UInt32)),
    b Array(String)
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_types VALUES (1, ['x', 'y'], [1, NULL], [[1], [2]], [(1), (2)], ['p']);

SELECT 'Element type wrappers';
SELECT count() FROM t_types ARRAY JOIN lc, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_types ARRAY JOIN nl, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_types ARRAY JOIN aa, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_types ARRAY JOIN tp, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- A wrapped element type is substituted like any other: only its size is read. The last row is the
-- live control for the Nullable(Int64) pattern, where nl IS read in full.
TRUNCATE TABLE t_types;
INSERT INTO t_types VALUES (1, ['x', 'y'], [1, NULL], [[1], [2]], [(1), (2)], ['p', 'q']);
SELECT count() FROM t_types ARRAY JOIN nl, b;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_types ARRAY JOIN nl, b)
    WHERE explain ILIKE '%nl.size0 UInt64%' SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM (EXPLAIN header = 1 SELECT count() FROM t_types ARRAY JOIN nl, b)
    WHERE explain ILIKE '%Nullable(Int64)%' SETTINGS enable_parallel_replicas = 0;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT nl FROM t_types ARRAY JOIN nl, b)
    WHERE explain ILIKE '%Nullable(Int64)%' SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_types;

-- A column literally named `b.size0` must not be mistaken for b's size subcolumn: the substituted
-- expression asks for the LENGTH of b and leaves resolving that to FunctionToSubcolumnsPass, which
-- declines the subcolumn here. Matching data must therefore NOT throw.
DROP TABLE IF EXISTS t_shadow;
CREATE TABLE t_shadow (id UInt32, a Array(String), b Array(String), `b.size0` UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shadow VALUES (1, ['x', 'y'], ['p', 'q'], 999);

SELECT 'Shadow size subcolumn';
SELECT count() FROM t_shadow ARRAY JOIN a, b;

TRUNCATE TABLE t_shadow;
INSERT INTO t_shadow VALUES (1, ['x', 'y'], ['p'], 999);
SELECT count() FROM t_shadow ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_shadow;

-- An ALIAS column operand resolves to its defining expression: matching sizes pass, and a mismatch
-- still throws through the ALIAS indirection.
DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (id UInt32, a Array(String), b Array(String), c Array(String) ALIAS b) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias VALUES (1, ['x', 'y'], ['p', 'q']);

SELECT 'ALIAS column operand';
SELECT count() FROM t_alias ARRAY JOIN a, c;

TRUNCATE TABLE t_alias;
INSERT INTO t_alias VALUES (1, ['x', 'y'], ['p']);
SELECT count() FROM t_alias ARRAY JOIN a, c; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_alias;

SELECT 'Arrays from a subquery (no storage subcolumn)';
SELECT count() FROM (SELECT ['x', 'y'] AS a, ['p'] AS b) ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM (SELECT ['x', 'y'] AS a, ['p', 'q'] AS b) ARRAY JOIN a, b;

SELECT 'General expression operand';
SELECT count() FROM (SELECT ['x', 'y'] AS a, ['p'] AS b) ARRAY JOIN arrayMap(x -> upper(x), a) AS ua, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Unaligned mode keeps its documented behaviour: sizes are not required to match.
SELECT 'Unaligned mode';
SELECT count() FROM (SELECT ['x', 'y'] AS a, ['p'] AS b) ARRAY JOIN a, b SETTINGS enable_unaligned_array_join = 1;

-- Chained ARRAY JOIN: a later clause's operand may be defined in terms of an earlier clause's
-- output, so that output is used and must not be substituted.
DROP TABLE IF EXISTS t_chain;
CREATE TABLE t_chain (id UInt32, a Array(UInt32), b Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain VALUES (1, [3], [9]);

SELECT 'Chained ARRAY JOIN';
-- x is referenced by the second clause, so range(x) must see [3], not the carrier's zeros. The
-- second spelling projects both operands and is the control.
SELECT count() FROM t_chain ARRAY JOIN a AS x, b AS z ARRAY JOIN range(x) AS y;
SELECT count() FROM (SELECT x, z FROM t_chain ARRAY JOIN a AS x, b AS z) ARRAY JOIN range(x) AS y;

TRUNCATE TABLE t_chain;
INSERT INTO t_chain VALUES (1, [3], [9, 9]);
SELECT count() FROM t_chain ARRAY JOIN a AS x, b AS z ARRAY JOIN range(x) AS y; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_chain;

-- A chained shape where the first clause's outputs are referenced nowhere: both are still
-- substituted, so only their sizes are read while the second clause's array is read in full.
DROP TABLE IF EXISTS t_chain_unused;
CREATE TABLE t_chain_unused (id UInt32, a Array(UInt32), b Array(UInt32), c Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain_unused VALUES (1, [3], [9], [1, 2]);

SELECT count() FROM t_chain_unused ARRAY JOIN a AS x, b AS z ARRAY JOIN c AS y;
-- The last row is the live control: c IS read in full, so a never-matching pattern would fail it.
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_chain_unused ARRAY JOIN a AS x, b AS z ARRAY JOIN c AS y)
    WHERE explain ILIKE '%a.size0 UInt64%' SETTINGS enable_parallel_replicas = 0;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_chain_unused ARRAY JOIN a AS x, b AS z ARRAY JOIN c AS y)
    WHERE explain ILIKE '%b.size0 UInt64%' SETTINGS enable_parallel_replicas = 0;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_chain_unused ARRAY JOIN a AS x, b AS z ARRAY JOIN c AS y)
    WHERE explain ILIKE '%c Array(UInt32)%' SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_chain_unused;

-- A Nested operand beside a plain array, with the Nested one entirely unused. It must be pruned by
-- subcolumn rather than substituted: wrapping nested() in length() would make that pruning
-- unreachable, so only the plain array contributes sizes while a single n.* subcolumn is still read.
DROP TABLE IF EXISTS t_nested_multi;
CREATE TABLE t_nested_multi (`n.a` Array(Int64), `n.b` Array(Int64), p Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_multi VALUES ([1, 2], [3, 4], [5, 6]);

SELECT 'Nested operand beside a plain array';
SELECT count() FROM t_nested_multi ARRAY JOIN n, p;
-- The first row is the live control for the Array(Int64) pattern.
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_nested_multi ARRAY JOIN n, p)
    WHERE explain ILIKE '%n.a Array(Int64)%' SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM (EXPLAIN header = 1 SELECT count() FROM t_nested_multi ARRAY JOIN n, p)
    WHERE explain ILIKE '%n.b%' SETTINGS enable_parallel_replicas = 0;
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count() FROM t_nested_multi ARRAY JOIN n, p)
    WHERE explain ILIKE '%p.size0 UInt64%' SETTINGS enable_parallel_replicas = 0;
-- The Nested operand still reaches the size check, so a mismatch must throw.
SELECT count() FROM t_nested_multi ARRAY JOIN n, arrayResize(p, 3) AS q; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_nested_multi;

-- The same verdicts with the subcolumn optimization disabled.
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_no_subcolumns;
CREATE TABLE t_no_subcolumns (id UInt32, a Array(String), b Array(String), `b.size0` UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_no_subcolumns VALUES (1, ['x', 'y'], ['p'], 999);

SELECT 'optimize_functions_to_subcolumns = 0';
SELECT count() FROM t_no_subcolumns ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id FROM t_no_subcolumns ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM t_no_subcolumns LEFT ARRAY JOIN a, b; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

TRUNCATE TABLE t_no_subcolumns;
INSERT INTO t_no_subcolumns VALUES (1, ['x', 'y'], ['p', 'q'], 999), (2, ['z'], ['r'], 999);
SELECT count() FROM t_no_subcolumns ARRAY JOIN a, b;
SELECT count() FROM t_no_subcolumns LEFT ARRAY JOIN a, b;

DROP TABLE t_no_subcolumns;

DROP TABLE IF EXISTS t_chain_no_subcolumns;
CREATE TABLE t_chain_no_subcolumns (id UInt32, a Array(UInt32), b Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain_no_subcolumns VALUES (1, [3], [9]);

SELECT 'Chained ARRAY JOIN, optimize_functions_to_subcolumns = 0';
SELECT count() FROM t_chain_no_subcolumns ARRAY JOIN a AS x, b AS z ARRAY JOIN range(x) AS y;
SELECT count() FROM (SELECT x, z FROM t_chain_no_subcolumns ARRAY JOIN a AS x, b AS z) ARRAY JOIN range(x) AS y;

TRUNCATE TABLE t_chain_no_subcolumns;
INSERT INTO t_chain_no_subcolumns VALUES (1, [3], [9, 9]);
SELECT count() FROM t_chain_no_subcolumns ARRAY JOIN a AS x, b AS z ARRAY JOIN range(x) AS y; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_chain_no_subcolumns;
