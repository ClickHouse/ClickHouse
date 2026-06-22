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
