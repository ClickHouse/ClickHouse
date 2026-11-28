-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending
-- Random settings limits: optimize_functions_to_subcolumns=(1, None); optimize_move_to_prewhere=(1, None); query_plan_optimize_prewhere=(1, None)

-- A query applying pending mutations on the fly must read a column the mutation consumed, such as a subcolumn its
-- expression reads, and answer as it does once the mutation is materialized.

SET alter_sync = 0, mutations_sync = 0, lightweight_deletes_sync = 0;
SET apply_mutations_on_fly = 1;

SELECT 'subcolumn kinds';

DROP TABLE IF EXISTS t_kinds;

CREATE TABLE t_kinds
(
    id UInt8,
    a Array(UInt32),
    x Nullable(UInt32),
    tu Tuple(p UInt32, q UInt32),
    mp Map(String, UInt32),
    j JSON,
    n Nested(e Int32),
    v Variant(UInt32, String),
    k1 UInt64,
    k2 UInt8,
    k3 UInt32,
    k4 UInt64,
    k5 String,
    k6 UInt64,
    k7 Nullable(UInt32),
    y UInt8
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_kinds (id, a, x, tu, mp, j, n.e, v) VALUES
    (1, [1, 2], NULL, (3, 4), map('k1', 1, 'k2', 2), '{"f":"hello"}', [10, 20, 30], 7),
    (2, [5], 8, (9, 9), map(), '{"f":42}', [], 's');

SYSTEM STOP MERGES t_kinds;

ALTER TABLE t_kinds UPDATE k1 = a.size0, k2 = x.null, k3 = tu.p, k4 = length(mp.keys), k5 = toString(j.f), k6 = n.e.size0, k7 = v.UInt32 WHERE 1;
ALTER TABLE t_kinds UPDATE y = 1 WHERE 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_kinds' AND NOT is_done;
SELECT 'pending', id, a.size0, k1, x.null, k2, tu.p, k3, mp.keys, k4, j.f, k5, n.e.size0, k6, v.UInt32, k7, y, length(a) FROM t_kinds ORDER BY id;
SELECT 'pending', id, k1 FROM t_kinds PREWHERE a.size0 = 2 ORDER BY id;
SELECT 'pending', id, k1 FROM t_kinds WHERE length(a) = 2 ORDER BY id;
SELECT 'pending', id, a.size0 FROM t_kinds PREWHERE k1 = 2 ORDER BY id;

SYSTEM START MERGES t_kinds;
ALTER TABLE t_kinds UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_kinds' AND NOT is_done;
SELECT 'materialized', id, a.size0, k1, x.null, k2, tu.p, k3, mp.keys, k4, j.f, k5, n.e.size0, k6, v.UInt32, k7, y, length(a) FROM t_kinds ORDER BY id;
SELECT 'materialized', id, k1 FROM t_kinds PREWHERE a.size0 = 2 ORDER BY id;
SELECT 'materialized', id, k1 FROM t_kinds WHERE length(a) = 2 ORDER BY id;
SELECT 'materialized', id, a.size0 FROM t_kinds PREWHERE k1 = 2 ORDER BY id;

DROP TABLE t_kinds;

SELECT 'predicates reading a subcolumn';

DROP TABLE IF EXISTS t_predicates;

CREATE TABLE t_predicates (id UInt8, a Array(UInt32), b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G';

INSERT INTO t_predicates VALUES (1, [1, 2], 0), (2, [], 0), (3, [5], 0), (4, [6, 7, 8], 0);

SYSTEM STOP MERGES t_predicates;

ALTER TABLE t_predicates DELETE WHERE a.size0 = 0;
ALTER TABLE t_predicates UPDATE b = 5 WHERE a.size0 = 2;
DELETE FROM t_predicates WHERE a.size0 = 3;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_predicates' AND NOT is_done;
SELECT 'pending', id, a.size0, b FROM t_predicates ORDER BY id;
SELECT 'pending', id, a.size0 FROM t_predicates ORDER BY id;

SYSTEM START MERGES t_predicates;
ALTER TABLE t_predicates UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_predicates' AND NOT is_done;
SELECT 'materialized', id, a.size0, b FROM t_predicates ORDER BY id;
SELECT 'materialized', id, a.size0 FROM t_predicates ORDER BY id;

DROP TABLE t_predicates;

SELECT 'column read before the mutations';

DROP TABLE IF EXISTS t_row_exists;

CREATE TABLE t_row_exists (id UInt8, b UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_row_exists VALUES (1, 0), (2, 0), (3, 0);

SET lightweight_deletes_sync = 2;
DELETE FROM t_row_exists WHERE id = 2;
SET lightweight_deletes_sync = 0;

SYSTEM STOP MERGES t_row_exists;

ALTER TABLE t_row_exists UPDATE b = 7 WHERE 1;
DELETE FROM t_row_exists WHERE id = 3;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_row_exists' AND NOT is_done;
SELECT 'pending', _row_exists, id, b FROM t_row_exists ORDER BY id;

SYSTEM START MERGES t_row_exists;
ALTER TABLE t_row_exists UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_row_exists' AND NOT is_done;
SELECT 'materialized', _row_exists, id, b FROM t_row_exists ORDER BY id;

DROP TABLE t_row_exists;

SELECT 'non-adaptive granularity';

DROP TABLE IF EXISTS t_non_adaptive;

CREATE TABLE t_non_adaptive (id UInt8, a Array(UInt32), b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_non_adaptive VALUES (1, [1, 2], 0), (2, [3], 0);

SYSTEM STOP MERGES t_non_adaptive;

ALTER TABLE t_non_adaptive UPDATE b = a.size0 WHERE 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_non_adaptive' AND NOT is_done;
SELECT 'pending', id, a.size0, b FROM t_non_adaptive ORDER BY id;

SYSTEM START MERGES t_non_adaptive;
ALTER TABLE t_non_adaptive UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_non_adaptive' AND NOT is_done;
SELECT 'materialized', id, a.size0, b FROM t_non_adaptive ORDER BY id;

DROP TABLE t_non_adaptive;

SELECT 'parent added after the part was written';

DROP TABLE IF EXISTS t_added;

CREATE TABLE t_added (id UInt8, b UInt64, c UInt8) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_added VALUES (1, 0, 0), (2, 0, 0);

ALTER TABLE t_added ADD COLUMN a Array(UInt32), ADD COLUMN x Nullable(UInt32) SETTINGS alter_sync = 2;

SYSTEM STOP MERGES t_added;

ALTER TABLE t_added UPDATE a = [7, 8, 9], x = 5, b = a.size0, c = x.null WHERE id = 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_added' AND NOT is_done;
SELECT 'pending', id, a.size0, b, x.null, c FROM t_added ORDER BY id;
SELECT 'pending', id, b FROM t_added PREWHERE a.size0 = 3 ORDER BY id;
SELECT 'pending', id, length(a), b FROM t_added ORDER BY id;

SYSTEM START MERGES t_added;
ALTER TABLE t_added UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_added' AND NOT is_done;
SELECT 'materialized', id, a.size0, b, x.null, c FROM t_added ORDER BY id;
SELECT 'materialized', id, b FROM t_added PREWHERE a.size0 = 3 ORDER BY id;
SELECT 'materialized', id, length(a), b FROM t_added ORDER BY id;

DROP TABLE t_added;

SELECT 'parent added with a default and rewritten by a later mutation';

DROP TABLE IF EXISTS t_added_default;

CREATE TABLE t_added_default (id UInt8, b UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_added_default VALUES (1, 0), (2, 0);

ALTER TABLE t_added_default ADD COLUMN a Array(UInt32) DEFAULT [1, 2] SETTINGS alter_sync = 2;

SYSTEM STOP MERGES t_added_default;

ALTER TABLE t_added_default UPDATE b = a.size0 WHERE 1;
ALTER TABLE t_added_default UPDATE a = [7, 8, 9] WHERE id = 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_added_default' AND NOT is_done;
SELECT 'pending', id, a.size0, b FROM t_added_default ORDER BY id;

SYSTEM START MERGES t_added_default;
ALTER TABLE t_added_default UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_added_default' AND NOT is_done;
SELECT 'materialized', id, a.size0, b FROM t_added_default ORDER BY id;

DROP TABLE t_added_default;

SET enable_lightweight_update = 1;

SELECT 'lightweight update older than the mutation';

DROP TABLE IF EXISTS t_patch_older;

CREATE TABLE t_patch_older (id UInt8, a Array(UInt32), x Nullable(UInt32), b UInt64, c UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_patch_older VALUES (1, [1, 2], NULL, 0, 0), (2, [3], 4, 0, 0);

SYSTEM STOP MERGES t_patch_older;

UPDATE t_patch_older SET a = [1, 1, 1], x = 5 WHERE id = 1;
ALTER TABLE t_patch_older UPDATE b = a.size0, c = x.null WHERE 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_older' AND NOT is_done;
SELECT 'pending', id, a.size0, b, x.null, c FROM t_patch_older ORDER BY id;
SELECT 'pending', id, b FROM t_patch_older PREWHERE a.size0 = 3 ORDER BY id;
SELECT 'pending', id, a.size0 FROM t_patch_older PREWHERE b = 3 ORDER BY id;
SELECT 'pending', id, length(a), b FROM t_patch_older ORDER BY id;

SYSTEM START MERGES t_patch_older;
ALTER TABLE t_patch_older UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_patch_older FINAL;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_older' AND NOT is_done;
SELECT 'materialized', id, a.size0, b, x.null, c FROM t_patch_older ORDER BY id;
SELECT 'materialized', id, b FROM t_patch_older PREWHERE a.size0 = 3 ORDER BY id;
SELECT 'materialized', id, a.size0 FROM t_patch_older PREWHERE b = 3 ORDER BY id;
SELECT 'materialized', id, length(a), b FROM t_patch_older ORDER BY id;

DROP TABLE t_patch_older;

SELECT 'lightweight update newer than the mutation';

DROP TABLE IF EXISTS t_patch_newer;

CREATE TABLE t_patch_newer (id UInt8, a Array(UInt32), b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_patch_newer VALUES (1, [1, 2], 0), (2, [3], 0);

SYSTEM STOP MERGES t_patch_newer;

ALTER TABLE t_patch_newer UPDATE b = a.size0 WHERE 1;
UPDATE t_patch_newer SET a = [1, 1, 1] WHERE id = 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_newer' AND NOT is_done;
SELECT 'pending', id, a.size0, b FROM t_patch_newer ORDER BY id;

SYSTEM START MERGES t_patch_newer;
ALTER TABLE t_patch_newer UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_patch_newer FINAL;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_newer' AND NOT is_done;
SELECT 'materialized', id, a.size0, b FROM t_patch_newer ORDER BY id;

DROP TABLE t_patch_newer;

SELECT 'lightweight updates before and between two mutations';

DROP TABLE IF EXISTS t_patch_between;

CREATE TABLE t_patch_between (id UInt8, a Array(UInt32), b UInt64, y UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_patch_between VALUES (1, [1, 2], 0, 0), (2, [3], 0, 0);

SYSTEM STOP MERGES t_patch_between;

UPDATE t_patch_between SET a = [1, 1, 1] WHERE id = 1;
ALTER TABLE t_patch_between UPDATE b = a.size0 WHERE id = 1;
UPDATE t_patch_between SET a = [5, 5] WHERE id = 2;
ALTER TABLE t_patch_between UPDATE y = 1 WHERE 1;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_between' AND NOT is_done;
SELECT 'pending', id, a.size0, b, y FROM t_patch_between ORDER BY id;
SELECT 'pending', id, b, y FROM t_patch_between PREWHERE a.size0 = 2 ORDER BY id;

SYSTEM START MERGES t_patch_between;
ALTER TABLE t_patch_between UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_patch_between FINAL;

SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_between' AND NOT is_done;
SELECT 'materialized', id, a.size0, b, y FROM t_patch_between ORDER BY id;
SELECT 'materialized', id, b, y FROM t_patch_between PREWHERE a.size0 = 2 ORDER BY id;

DROP TABLE t_patch_between;
