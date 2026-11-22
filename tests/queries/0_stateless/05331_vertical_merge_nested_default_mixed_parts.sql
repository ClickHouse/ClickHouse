-- Vertical merge of Nested arrays with DEFAULT expressions referencing
-- non-existent sibling columns must not corrupt data.
-- https://github.com/ClickHouse/ClickHouse/issues/86123
-- The cases are split across `04050_vertical_merge_nested_default_corruption`,
-- `05331_vertical_merge_nested_default_mixed_parts` and `05332_vertical_merge_nested_default_patch_parts`
-- to keep each test fast enough.

-- The transitive chain of `t_nested_default_chain` (in `04050_vertical_merge_nested_default_corruption`), but with *mixed* source parts: the intermediate `tmp` is materialized in
-- one source part (by a mutation that touches only the rows of that part) and still missing in the
-- other. The merge has to write `tmp` anyway, so the dependency closure stops at it and
-- `` `n.b` `` is materialized from the merged `tmp`. For the rows of the part where `tmp` is
-- recomputed from the expired `m`, the value of `` `n.b` `` is reconciled with the shared Nested
-- offsets, so the offsets of the present sibling `n.a` are not corrupted.
-- `max_bytes_to_merge_at_max_space_in_pool` keeps background merges from consuming the mixed parts
-- before `OPTIMIZE ... FINAL` (which ignores that limit).
DROP TABLE IF EXISTS t_nested_mixed_parts;

CREATE TABLE t_nested_mixed_parts (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_mixed_parts VALUES (1, [10,20]);
INSERT INTO t_nested_mixed_parts VALUES (2, [30,40]);

ALTER TABLE t_nested_mixed_parts ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_mixed_parts ADD COLUMN tmp Array(String) DEFAULT arrayMap(v -> toString(v), m);
ALTER TABLE t_nested_mixed_parts ADD COLUMN `n.b` Array(String) DEFAULT tmp;

-- Touches only the part holding `id = 1`, so `tmp` becomes stored there and stays missing elsewhere.
ALTER TABLE t_nested_mixed_parts UPDATE tmp = tmp WHERE id = 1 SETTINGS mutations_sync = 2;

-- Exactly one of the two active parts stores `tmp` (the mixed state this case is about).
SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_parts' AND active AND column = 'tmp';

OPTIMIZE TABLE t_nested_mixed_parts FINAL;

-- `` `n.b` `` is written by the merge: it has a row in `system.parts_columns`.
SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_parts' AND active AND column = 'n.b';

-- The merge must complete without corrupting the shared Nested offsets, so the sibling survives.
SELECT count(), countDistinct(_part) FROM t_nested_mixed_parts;
SELECT id, `n.a`, length(`n.b`) FROM t_nested_mixed_parts ORDER BY id;
CHECK TABLE t_nested_mixed_parts SETTINGS check_query_single_value_result = 1;

DROP TABLE t_nested_mixed_parts;

-- The same mixed state with a non-deterministic outer default. The merge writes `tmp`, so
-- `` `n.b` `` must be materialized from it rather than expired: an expired `` `n.b` `` would be
-- recomputed on every read and return different values each time, while the merge can freeze one
-- value. `tmp` evaluates to the sizes of `n.a`, so no reconciliation is needed here.
DROP TABLE IF EXISTS t_nested_mixed_parts_rand;

CREATE TABLE t_nested_mixed_parts_rand (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_mixed_parts_rand VALUES (1, [10,20]);
INSERT INTO t_nested_mixed_parts_rand VALUES (2, [30,40]);

ALTER TABLE t_nested_mixed_parts_rand ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_mixed_parts_rand ADD COLUMN tmp Array(UInt32) DEFAULT arrayConcat(m, `n.a`);
ALTER TABLE t_nested_mixed_parts_rand ADD COLUMN `n.b` Array(UInt64) DEFAULT arrayMap(x -> rand64(x), tmp);

-- Touches only the part holding `id = 1`, so `tmp` becomes stored there and stays missing elsewhere.
ALTER TABLE t_nested_mixed_parts_rand UPDATE tmp = tmp WHERE id = 1 SETTINGS mutations_sync = 2;

SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_parts_rand' AND active AND column = 'tmp';

OPTIMIZE TABLE t_nested_mixed_parts_rand FINAL;

SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_parts_rand' AND active AND column = 'n.b';

SELECT count(), countDistinct(_part) FROM t_nested_mixed_parts_rand;
SELECT id, `n.a`, tmp, length(`n.b`) FROM t_nested_mixed_parts_rand ORDER BY id;

-- Two reads return the same frozen values.
SELECT (SELECT groupArray(`n.b`) FROM (SELECT `n.b` FROM t_nested_mixed_parts_rand ORDER BY id))
     = (SELECT groupArray(`n.b`) FROM (SELECT `n.b` FROM t_nested_mixed_parts_rand ORDER BY id));
CHECK TABLE t_nested_mixed_parts_rand SETTINGS check_query_single_value_result = 1;

DROP TABLE t_nested_mixed_parts_rand;

-- Mixed source parts for the Nested subcolumn *itself*: one part stores `` `n.b` `` (inserted
-- explicitly after the ALTERs), the other still relies on its DEFAULT, whose dependency `m` is
-- absent from that older part. `` `n.b` `` cannot be expired - that would drop the stored values -
-- and its DEFAULT cannot be evaluated faithfully for the older part's rows (the absent `m`
-- degrades to an empty literal), so the evaluated value would have array sizes inconsistent with
-- the shared Nested offsets. The merge must reconcile the evaluated default with the offsets the
-- part already stores: the rows of the older part degrade to type-default values shaped by the
-- sibling's offsets, and the stored values of the newer part survive as-is.
DROP TABLE IF EXISTS t_nested_mixed_stored;

CREATE TABLE t_nested_mixed_stored (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_mixed_stored VALUES (1, [10,20]);

ALTER TABLE t_nested_mixed_stored ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_mixed_stored ADD COLUMN `n.b` Array(String) DEFAULT arrayMap(v -> toString(v), m);

INSERT INTO t_nested_mixed_stored (id, `n.a`, `n.b`) VALUES (2, [30,40], ['x','y']);

-- Exactly one of the two active parts stores `n.b` (the mixed state this case is about).
SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_stored' AND active AND column = 'n.b';

OPTIMIZE TABLE t_nested_mixed_stored FINAL;

-- `n.b` stays materialized (it cannot be expired without losing the stored values).
SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_mixed_stored' AND active AND column = 'n.b';
SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_nested_mixed_stored' AND active;

-- The stored values survive, the recomputed rows agree with the shared offsets.
SELECT id, `n.a`, `n.b` FROM t_nested_mixed_stored ORDER BY id;

DROP TABLE t_nested_mixed_stored;

-- The same mixed state merged with the horizontal algorithm: reading all columns together does not
-- make the DEFAULT evaluable either (`m` is still absent from the older part), so the evaluated
-- value must be reconciled with the shared offsets all the same.
DROP TABLE IF EXISTS t_nested_mixed_stored_horizontal;

CREATE TABLE t_nested_mixed_stored_horizontal (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    enable_vertical_merge_algorithm = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_mixed_stored_horizontal VALUES (1, [10,20]);

ALTER TABLE t_nested_mixed_stored_horizontal ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_mixed_stored_horizontal ADD COLUMN `n.b` Array(String) DEFAULT arrayMap(v -> toString(v), m);

INSERT INTO t_nested_mixed_stored_horizontal (id, `n.a`, `n.b`) VALUES (2, [30,40], ['x','y']);

OPTIMIZE TABLE t_nested_mixed_stored_horizontal FINAL;

SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_nested_mixed_stored_horizontal' AND active;
SELECT id, `n.a`, `n.b` FROM t_nested_mixed_stored_horizontal ORDER BY id;

DROP TABLE t_nested_mixed_stored_horizontal;

-- A DEFAULT with no dependencies at all whose value has array sizes different from the shared
-- Nested offsets (a plain constant). The reconciliation is per row: a row whose evaluated sizes
-- happen to agree with the sibling's offsets keeps the evaluated value, a row whose sizes disagree
-- degrades to the type-default value shaped by the offsets.
DROP TABLE IF EXISTS t_nested_const_default;

CREATE TABLE t_nested_const_default (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_const_default;

INSERT INTO t_nested_const_default VALUES (1, [10]);
INSERT INTO t_nested_const_default VALUES (2, [30,40]);

ALTER TABLE t_nested_const_default ADD COLUMN `n.b` Array(String) DEFAULT ['x'];

SYSTEM START MERGES t_nested_const_default;
OPTIMIZE TABLE t_nested_const_default FINAL;

SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_nested_const_default' AND active;
SELECT id, `n.a`, `n.b` FROM t_nested_const_default ORDER BY id;

DROP TABLE t_nested_const_default;
