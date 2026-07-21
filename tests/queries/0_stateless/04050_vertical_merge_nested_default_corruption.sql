-- Vertical merge of Nested arrays with DEFAULT expressions referencing
-- non-existent sibling columns must not corrupt data.
-- https://github.com/ClickHouse/ClickHouse/issues/86123

DROP TABLE IF EXISTS t_nested_vertical;

CREATE TABLE t_nested_vertical (
    id UInt32,
    `n1.nums` Array(UInt32),
    `n1.numsplus` Array(UInt32),
    `n2.nums` Array(UInt32),
    `n2.numsplus` Array(UInt32)
) ENGINE = ReplacingMergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_vertical;

INSERT INTO t_nested_vertical VALUES (1, [1,1,1], [2,2,2], [11,11,11], [22,22,22]);
INSERT INTO t_nested_vertical VALUES (2, [2,2,2], [3,3,3], [22,22,22], [33,33,33]);
INSERT INTO t_nested_vertical VALUES (3, [3,3,3], [4,4,4], [33,33,33], [44,44,44]);
INSERT INTO t_nested_vertical VALUES (4, [4,4,4], [5,5,5], [44,44,44], [55,55,55]);

ALTER TABLE t_nested_vertical ADD COLUMN `n1.urls` Array(Array(String));
ALTER TABLE t_nested_vertical ADD COLUMN `n1.domains` Array(Array(String))
    DEFAULT arrayMap(x -> arrayMap(y -> domain(y), CAST(x AS Array(String))), `n1.urls`);

-- Verify data is intact before merge
SELECT id, `n1.nums`, `n1.numsplus`, `n2.nums`, `n2.numsplus` FROM t_nested_vertical ORDER BY id;

SYSTEM START MERGES t_nested_vertical;
OPTIMIZE TABLE t_nested_vertical FINAL;

-- After vertical merge, Nested arrays must not be corrupted
SELECT id, `n1.nums`, `n1.numsplus`, `n2.nums`, `n2.numsplus` FROM t_nested_vertical ORDER BY id;

-- The new columns should return correct defaults (empty arrays matching Nested dimensions)
SELECT id, `n1.urls`, `n1.domains` FROM t_nested_vertical ORDER BY id;

DROP TABLE t_nested_vertical;

-- Multi-hop transitive dependency chain: c3 -> c2 -> c1 (expired)
DROP TABLE IF EXISTS t_nested_chain;

CREATE TABLE t_nested_chain (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_chain;

INSERT INTO t_nested_chain VALUES (1, [10,20]);
INSERT INTO t_nested_chain VALUES (2, [30,40]);

ALTER TABLE t_nested_chain ADD COLUMN `n.b` Array(String);
ALTER TABLE t_nested_chain ADD COLUMN `n.c` Array(String) DEFAULT `n.b`;
ALTER TABLE t_nested_chain ADD COLUMN `n.d` Array(String) DEFAULT `n.c`;

SYSTEM START MERGES t_nested_chain;
OPTIMIZE TABLE t_nested_chain FINAL;

SELECT id, `n.a`, `n.b`, `n.c`, `n.d` FROM t_nested_chain ORDER BY id;

DROP TABLE t_nested_chain;

-- A Nested subcolumn whose DEFAULT references a subcolumn (`m.keys`) of an expired column (`m`).
-- `collectIdentifierNames` returns the subcolumn name `m.keys`, while the expired set holds the
-- physical storage column name `m`. The identifier must be resolved back to its storage column
-- before the lookup; otherwise `n.b` is not expired and the vertical merge fails to materialize
-- the missing subcolumn (writing Nested offsets inconsistent with its sibling `n.a`).
-- The merge must complete and the sibling `n.a` must remain intact.
DROP TABLE IF EXISTS t_nested_subcol;

CREATE TABLE t_nested_subcol (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_subcol;

INSERT INTO t_nested_subcol VALUES (1, [10,20]);
INSERT INTO t_nested_subcol VALUES (2, [30,40]);

ALTER TABLE t_nested_subcol ADD COLUMN m Map(String, String);
ALTER TABLE t_nested_subcol ADD COLUMN `n.b` Array(String) DEFAULT m.keys;

SYSTEM START MERGES t_nested_subcol;
OPTIMIZE TABLE t_nested_subcol FINAL;

-- Sibling data must survive the merge into a single part.
-- (`n.b` itself is not selected: a DEFAULT referencing a subcolumn of a missing column is a
--  separate, pre-existing read-path limitation independent of the merge corruption fixed here.)
SELECT count(), countDistinct(_part) FROM t_nested_subcol;
SELECT id, `n.a` FROM t_nested_subcol ORDER BY id;

DROP TABLE t_nested_subcol;

-- A lambda formal parameter must not be mistaken for a column dependency. The DEFAULT of the Nested
-- subcolumn `n.b` uses a lambda whose parameter `x` shadows the expired physical column `x`, but the
-- expression only reads the present sibling `n.a`. A naive identifier walk would treat the
-- lambda-local `x` as the expired column and wrongly expire `n.b`, dropping it from the merged part.
-- Scope-aware dependency analysis excludes lambda parameters, so `n.b` stays materialized.
DROP TABLE IF EXISTS t_nested_lambda_shadow;

CREATE TABLE t_nested_lambda_shadow (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_lambda_shadow;

INSERT INTO t_nested_lambda_shadow VALUES (1, [10,20]);
INSERT INTO t_nested_lambda_shadow VALUES (2, [30,40]);

ALTER TABLE t_nested_lambda_shadow ADD COLUMN x UInt32;
ALTER TABLE t_nested_lambda_shadow ADD COLUMN `n.b` Array(UInt32) DEFAULT arrayMap(x -> x + 1, `n.a`);

SYSTEM START MERGES t_nested_lambda_shadow;
OPTIMIZE TABLE t_nested_lambda_shadow FINAL;

-- Exactly one part after the merge, and `n.b` must be materialized in it (not wrongly expired).
SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_nested_lambda_shadow' AND active;
SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_lambda_shadow' AND active AND column = 'n.b';

-- Sibling data intact and the default evaluates from the present `n.a`.
SELECT id, `n.a`, `n.b` FROM t_nested_lambda_shadow ORDER BY id;

DROP TABLE t_nested_lambda_shadow;

-- A DEFAULT dependency reached only through an ALIAS column must still be discovered. `s` is an
-- expired physical column, `s_alias` is `ALIAS s`, and the Nested subcolumn `n.b` depends on the
-- expired `s` only via the alias. Without expanding aliases, `n.b` is not expired and vertical merge
-- tries to materialize it from the missing `s`, writing Nested offsets inconsistent with the present
-- sibling `n.a` and corrupting the shared offsets. Expanding the alias expires `n.b` instead.
DROP TABLE IF EXISTS t_nested_alias;

CREATE TABLE t_nested_alias (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_alias;

INSERT INTO t_nested_alias VALUES (1, [10,20]);
INSERT INTO t_nested_alias VALUES (2, [30,40]);

ALTER TABLE t_nested_alias ADD COLUMN s Array(String);
ALTER TABLE t_nested_alias ADD COLUMN s_alias Array(String) ALIAS s;
ALTER TABLE t_nested_alias ADD COLUMN `n.b` Array(String) DEFAULT s_alias;

SYSTEM START MERGES t_nested_alias;
OPTIMIZE TABLE t_nested_alias FINAL;

-- The merge must complete without corrupting the shared Nested offsets, so the sibling survives.
SELECT count(), countDistinct(_part) FROM t_nested_alias;
SELECT id, `n.a` FROM t_nested_alias ORDER BY id;

DROP TABLE t_nested_alias;

-- A DEFAULT dependency reached through a *subcolumn of an ALIAS column* must still be discovered.
-- `m` is an expired physical `Map` column, `m_alias` is `ALIAS m`, and the Nested subcolumn `n.b`
-- depends on the expired `m` only via the alias subcolumn `m_alias.keys`. `ColumnsDescription` does
-- not register subcolumns of alias columns, so `m_alias.keys` does not resolve directly; the
-- dependency must be recovered by resolving the identifier prefix (`m_alias`) and expanding the alias
-- to the physical `m`. Without this, `n.b` is not expired and vertical merge tries to materialize it
-- from the missing `m`, corrupting the shared Nested offsets of the present sibling `n.a`.
DROP TABLE IF EXISTS t_nested_alias_subcol;

CREATE TABLE t_nested_alias_subcol (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_alias_subcol;

INSERT INTO t_nested_alias_subcol VALUES (1, [10,20]);
INSERT INTO t_nested_alias_subcol VALUES (2, [30,40]);

ALTER TABLE t_nested_alias_subcol ADD COLUMN m Map(String, String);
ALTER TABLE t_nested_alias_subcol ADD COLUMN m_alias Map(String, String) ALIAS m;
ALTER TABLE t_nested_alias_subcol ADD COLUMN `n.b` Array(String) DEFAULT m_alias.keys;

SYSTEM START MERGES t_nested_alias_subcol;
OPTIMIZE TABLE t_nested_alias_subcol FINAL;

-- The merge must complete without corrupting the shared Nested offsets, so the sibling survives.
-- (`n.b` itself is not selected: a DEFAULT reading a subcolumn of a missing column is the same
--  pre-existing read-path limitation noted for `t_nested_subcol` above.)
SELECT count(), countDistinct(_part) FROM t_nested_alias_subcol;
SELECT id, `n.a` FROM t_nested_alias_subcol ORDER BY id;

DROP TABLE t_nested_alias_subcol;

-- A DEFAULT dependency reached through a subcolumn of an ALIAS column whose *name contains a dot*
-- must still be discovered. `m` is an expired physical `Map` column, `` `x.y` `` is `ALIAS m` (a legal
-- quoted column name that itself contains a dot), and the Nested subcolumn `n.b` depends on the
-- expired `m` only via the alias subcolumn `` `x.y`.keys ``. Resolving the identifier to its storage
-- column must find the longest existing column-name prefix (`` `x.y` ``), not merely peel the first
-- dotted segment (`x`); otherwise the alias is never expanded, `n.b` is not expired, and vertical
-- merge corrupts the shared Nested offsets of the present sibling `n.a`.
DROP TABLE IF EXISTS t_nested_alias_dotted;

CREATE TABLE t_nested_alias_dotted (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_alias_dotted;

INSERT INTO t_nested_alias_dotted VALUES (1, [10,20]);
INSERT INTO t_nested_alias_dotted VALUES (2, [30,40]);

ALTER TABLE t_nested_alias_dotted ADD COLUMN m Map(String, String);
ALTER TABLE t_nested_alias_dotted ADD COLUMN `x.y` Map(String, String) ALIAS m;
ALTER TABLE t_nested_alias_dotted ADD COLUMN `n.b` Array(String) DEFAULT `x.y`.keys;

SYSTEM START MERGES t_nested_alias_dotted;
OPTIMIZE TABLE t_nested_alias_dotted FINAL;

-- The merge must complete without corrupting the shared Nested offsets, so the sibling survives.
SELECT count(), countDistinct(_part) FROM t_nested_alias_dotted;
SELECT id, `n.a` FROM t_nested_alias_dotted ORDER BY id;

DROP TABLE t_nested_alias_dotted;

-- A transitive dependency reached through a missing ordinary `DEFAULT` intermediate must still be
-- discovered. `m` is an expired physical column (absent from every source part, no default), `tmp`
-- is an ordinary, non-`ALIAS`, non-`Nested` column whose `DEFAULT` reads the expired `m`, and the
-- Nested subcolumn `n.b` is `` DEFAULT tmp ``. During the merge `tmp` is itself recomputed from its
-- expression, so `n.b` transitively depends on the expired `m`. The dependency closure must follow
-- the missing default-bearing intermediate `tmp` and expire `n.b`; otherwise vertical merge
-- materializes `n.b` from the recomputed (empty) `tmp`, writing Nested offsets inconsistent with the
-- present sibling `n.a` and corrupting the shared offsets.
DROP TABLE IF EXISTS t_nested_default_chain;

CREATE TABLE t_nested_default_chain (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_default_chain;

INSERT INTO t_nested_default_chain VALUES (1, [10,20]);
INSERT INTO t_nested_default_chain VALUES (2, [30,40]);

ALTER TABLE t_nested_default_chain ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_default_chain ADD COLUMN tmp Array(String) DEFAULT arrayMap(v -> toString(v), m);
ALTER TABLE t_nested_default_chain ADD COLUMN `n.b` Array(String) DEFAULT tmp;

SYSTEM START MERGES t_nested_default_chain;
OPTIMIZE TABLE t_nested_default_chain FINAL;

-- The merge must complete without corrupting the shared Nested offsets, so the sibling survives.
SELECT count(), countDistinct(_part) FROM t_nested_default_chain;
SELECT id, `n.a` FROM t_nested_default_chain ORDER BY id;

DROP TABLE t_nested_default_chain;
