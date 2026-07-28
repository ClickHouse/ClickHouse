-- `StorageMerge::supportedPrewhereColumns` compares the root type only against each child's
-- *declared* columns. A nested `Merge` can declare a matching type while its own leaf differs, so
-- PREWHERE was admitted, built against the root type, then re-derived against the leaf's type -
-- `ActionsDAG` then aborted with `Unexpected return type from notEquals. Expected Nullable(UInt8).
-- Got UInt8`. The column must be rejected for PREWHERE transitively, like the single-level case.

DROP TABLE IF EXISTS t_leaf;
DROP TABLE IF EXISTS t_inner;
DROP TABLE IF EXISTS t_outer;

CREATE TABLE t_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_leaf SELECT number, number + 1 FROM numbers(10);

-- `x` is Nullable here but not in the leaf: the mismatch is one level below the outer table.
CREATE TABLE t_inner (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^t_leaf$');
CREATE TABLE t_outer (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^t_inner$');

SELECT '-- single level: the mismatched column is already rejected for PREWHERE --';
SELECT count() FROM t_inner PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- nested: must be rejected too, not abort in ActionsDAG --';
SELECT count() FROM t_outer PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a column whose type matches all the way down still supports PREWHERE --';
SELECT count() FROM t_outer PREWHERE y != 0;
-- Read the columns too: `count()` alone need not materialize them, so it would not exercise the
-- leaf's `UInt64` -> the root's `Nullable(UInt64)` conversion that the abort came from.
SELECT x, y FROM t_outer PREWHERE y != 0 ORDER BY x LIMIT 3;

SELECT '-- the same predicate as WHERE keeps working --';
SELECT count() FROM t_outer WHERE x != 0;
SELECT count() FROM t_outer WHERE y != 0;
SELECT x, y FROM t_outer WHERE x != 0 ORDER BY x LIMIT 3;

DROP TABLE t_outer;
DROP TABLE t_inner;
DROP TABLE t_leaf;
