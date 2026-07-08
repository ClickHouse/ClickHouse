-- Referencing a virtual column whose value is only produced by the query plan
-- (_sample_factor, _table, _database) in a mutation must fail at analysis time,
-- not mid-execution in MergeTreeSequentialSource. See issue #78465.

DROP TABLE IF EXISTS t_mut_qp_virtuals;

CREATE TABLE t_mut_qp_virtuals (c0 UInt32) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_mut_qp_virtuals VALUES (1), (2), (3);

SET mutations_sync = 2;

-- These are rejected up front (NO_SUCH_COLUMN_IN_TABLE), the mutation never starts.
DELETE FROM t_mut_qp_virtuals WHERE _sample_factor > 0.1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DELETE FROM t_mut_qp_virtuals WHERE _table != ''; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DELETE FROM t_mut_qp_virtuals WHERE _database != ''; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_virtuals UPDATE c0 = 9 WHERE _sample_factor > 0.1; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_virtuals DELETE WHERE toFloat64(_table = '') > c0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- The value is only available in a SELECT, which keeps working.
SELECT _sample_factor FROM t_mut_qp_virtuals SAMPLE 0.5 LIMIT 1 FORMAT Null;
SELECT count() FROM t_mut_qp_virtuals WHERE _sample_factor >= 1.0;

-- Virtual columns that the mutation read path can materialize are still usable.
ALTER TABLE t_mut_qp_virtuals DELETE WHERE _part = 'nonexistent_part';
SELECT count() FROM t_mut_qp_virtuals;

DROP TABLE t_mut_qp_virtuals;
