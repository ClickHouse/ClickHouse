-- `ALTER TABLE ... CLEAR COLUMN` on a `Nested` `Tuple` member leaves the member absent from the
-- mutated part while the table metadata still declares it. Reading the member together with one of
-- its subcolumns then used to raise `Bad cast from type DB::ColumnVector<int> to DB::ColumnTuple` in
-- `fillMissingColumns`: the synthesized `Nested` group took the member's element type from the
-- subcolumn entry, which carries the metadata type rather than the type of the data the part
-- supplies. `ALTER TABLE ... ADD COLUMN` leaves the member absent in the same way and reaches the
-- same code path.
-- The pinned settings keep the read on that path: it needs a Compact part
-- (`min_bytes_for_wide_part` is randomized by the test runner) and shared `Nested` offsets.
-- `[(0)]` below is the fixture's own guard: an unapplied mutation would read `[(10)]`.

DROP TABLE IF EXISTS t_nested_cleared;
CREATE TABLE t_nested_cleared (c0 Nested(c1 Int32, c2 Tuple(c3 Int32)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_cleared (`c0.c1`, `c0.c2`) VALUES ([1], [(10)]);
ALTER TABLE t_nested_cleared CLEAR COLUMN `c0.c2` SETTINGS mutations_sync = 2;

SELECT '-- the cleared member and its `size0` subcolumn read together';
SELECT `c0.c2`, `c0.c2`.size0 FROM t_nested_cleared;
SELECT '-- the same pair in the other order';
SELECT `c0.c2`.size0, `c0.c2` FROM t_nested_cleared;
SELECT '-- the subcolumn alone was already correct and must stay so';
SELECT `c0.c2`.size0 FROM t_nested_cleared;

DROP TABLE t_nested_cleared;
