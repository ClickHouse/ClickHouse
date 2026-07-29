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

-- A subquery is evaluated as its own SELECT and can materialize these virtuals.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE c0 IN (SELECT c0 FROM t_mut_qp_virtuals WHERE _sample_factor > 100);

-- The value is only available in a SELECT, which keeps working.
SELECT _sample_factor FROM t_mut_qp_virtuals SAMPLE 0.5 LIMIT 1 FORMAT Null;
SELECT count() FROM t_mut_qp_virtuals WHERE _sample_factor >= 1.0;

-- Virtual columns that the mutation read path can materialize are still usable.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE _part = 'nonexistent_part';
SELECT count() FROM t_mut_qp_virtuals;

DROP TABLE t_mut_qp_virtuals;
