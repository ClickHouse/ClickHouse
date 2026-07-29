-- Referencing a virtual column whose value is only produced by the query plan
-- (_sample_factor, _table, _database) in a mutation must fail at analysis time,
-- not mid-execution in MergeTreeSequentialSource. See issue #78465.
-- The `ALIAS`-over-virtual cases need the analyzer, so they live in 04511.

DROP TABLE IF EXISTS t_mut_qp_virtuals;

CREATE TABLE t_mut_qp_virtuals (c0 UInt32, u Int32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_mut_qp_virtuals VALUES (1, 10, [1, 2]), (2, 20, [3]), (3, 30, [4]);

SET mutations_sync = 2;

-- These are rejected up front (NO_SUCH_COLUMN_IN_TABLE), the mutation never starts.
DELETE FROM t_mut_qp_virtuals WHERE _sample_factor > 0.1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DELETE FROM t_mut_qp_virtuals WHERE _table != ''; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DELETE FROM t_mut_qp_virtuals WHERE _database != ''; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_virtuals UPDATE u = 9 WHERE _sample_factor > 0.1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_virtuals DELETE WHERE toFloat64(_table = '') > c0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- The right-hand side of an UPDATE assignment is checked too.
ALTER TABLE t_mut_qp_virtuals UPDATE u = toInt32(_sample_factor) WHERE c0 > 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- A qualified reference resolves to the same virtual column and must still be rejected;
-- matching the raw identifier name (t._sample_factor) would miss this one.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE t_mut_qp_virtuals._sample_factor > 0.1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- A qualified reference must be rejected even when the table name collides with a real
-- column: `t._table` has a leading part that names a column, but the resolvers strip the
-- table qualifier and bind it to the virtual `_table`. Exempting on "first part is a column"
-- would let this bypass the gate and fail mid-execution.
DROP TABLE IF EXISTS t;
CREATE TABLE t (t UInt8) ENGINE = MergeTree ORDER BY t;
INSERT INTO t VALUES (1);
ALTER TABLE t DELETE WHERE t._table != ''; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t UPDATE t = toUInt8(t._sample_factor) WHERE t > 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- The plain column `t` on the same table still mutates fine.
ALTER TABLE t DELETE WHERE t = 99;
SELECT count() FROM t;
DROP TABLE t;

-- A genuine Tuple subcolumn whose short name collides with a virtual is a real column
-- access and must be allowed. `tup._table` reads the `_table` field of the Tuple column.
DROP TABLE IF EXISTS t_mut_qp_subcol;
CREATE TABLE t_mut_qp_subcol (c0 UInt32, v UInt32, tup Tuple(_table UInt32, _sample_factor Float64)) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_qp_subcol VALUES (1, 0, (10, 0.5)), (2, 0, (20, 1.5));
ALTER TABLE t_mut_qp_subcol DELETE WHERE tup._table = 999;
ALTER TABLE t_mut_qp_subcol UPDATE v = toUInt32(tup._sample_factor) WHERE c0 > 100;
SELECT count() FROM t_mut_qp_subcol;
DROP TABLE t_mut_qp_subcol;

-- A lambda formal parameter that merely shares the name is not a reference to the virtual
-- column and must be allowed (matching the raw identifier name would falsely reject it).
ALTER TABLE t_mut_qp_virtuals UPDATE arr = arrayMap(_table -> _table + 1, arr) WHERE c0 > 0;
ALTER TABLE t_mut_qp_virtuals UPDATE arr = arrayMap(_sample_factor -> _sample_factor * 2, arr) WHERE c0 > 0;

-- An expression alias defines the name for the whole expression, so the reference below binds
-- to the alias rather than to the virtual column.
ALTER TABLE t_mut_qp_virtuals UPDATE u = (1 AS _table) + _table WHERE c0 > 0;
SELECT DISTINCT u FROM t_mut_qp_virtuals;
-- An alias is visible to the whole command, so one assignment may use what another defines.
ALTER TABLE t_mut_qp_virtuals UPDATE u = (3 AS _table), arr = [toUInt32(_table)] WHERE c0 > 0;
SELECT DISTINCT u, arr FROM t_mut_qp_virtuals;
-- An alias is not visible inside the subtree that defines it, so this inner reference is the
-- virtual column: `SELECT (length(_table) AS _table)` returns the length of the table name.
-- The second condition must match only some rows, otherwise the whole part is dropped without
-- being read and the missing rejection would go unnoticed.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE (length(_table) AS _table) > 0 AND c0 > 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- A lambda body is its own alias scope, so an alias defined there must not hide the reference
-- in the enclosing predicate, which is still the virtual column.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE arrayExists(x -> (x AS _table) > 0, [1]) AND _table != '' AND c0 > 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- Inside that lambda body the alias is visible, so this reference binds to it and is allowed.
ALTER TABLE t_mut_qp_virtuals UPDATE arr = arrayMap(x -> (1 AS _table) + _table, arr) WHERE c0 > 0;
SELECT DISTINCT arr FROM t_mut_qp_virtuals;
-- An alias attached to a subquery node itself belongs to the enclosing scope, so it does hide
-- the name and the following reference is that alias, not the virtual column.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE ((SELECT 7) AS _table) > 0 AND _table > 6 AND c0 > 2;

-- A subquery is evaluated as its own SELECT and can materialize these virtuals.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE c0 IN (SELECT c0 FROM t_mut_qp_virtuals WHERE _sample_factor > 100);
-- An alias defined inside a subquery belongs to that subquery, so it must not hide the
-- reference in the enclosing predicate.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE _table != '' AND c0 IN (SELECT 1 AS _table); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- The value is only available in a SELECT, which keeps working.
SELECT _sample_factor FROM t_mut_qp_virtuals SAMPLE 0.5 LIMIT 1 FORMAT Null;
SELECT count() FROM t_mut_qp_virtuals WHERE _sample_factor >= 1.0;

-- Virtual columns that the mutation read path can materialize are still usable.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE _part = 'nonexistent_part';
SELECT count() FROM t_mut_qp_virtuals;

-- Every rejection above happened before the mutation was queued, so none of them left a failed
-- mutation behind: an alias-scope regression that let one through would show up here.
SELECT countIf(latest_fail_reason != '') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_qp_virtuals';

DROP TABLE t_mut_qp_virtuals;

-- A lightweight update reads through `ReadFromMergeTree`, which does supply these fields, so
-- it must keep working: the rejection applies only to the per-part mutation read path.
DROP TABLE IF EXISTS t_mut_qp_lightweight;
CREATE TABLE t_mut_qp_lightweight (c0 UInt32, v UInt32) ENGINE = MergeTree ORDER BY c0
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_mut_qp_lightweight SELECT number, 0 FROM numbers(4);
SET allow_experimental_lightweight_update = 1;
UPDATE t_mut_qp_lightweight SET v = length(_table) WHERE c0 < 2;
SELECT sum(v) FROM t_mut_qp_lightweight;
DROP TABLE t_mut_qp_lightweight;
