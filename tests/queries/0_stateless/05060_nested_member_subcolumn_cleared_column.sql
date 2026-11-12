-- `ALTER TABLE ... CLEAR COLUMN` leaves a `Nested` `Tuple` member absent from the mutated part while
-- the metadata still declares it; reading it beside one of its subcolumns then raised a logical error
-- in `fillMissingColumns`. `ALTER TABLE ... ADD COLUMN` leaves it absent the same way, same path.
-- The pinned settings keep the read on that path: it needs a Compact part
-- (`min_bytes_for_wide_part` is randomized by the test runner) and shared `Nested` offsets.
-- `[(0)]` below is the fixture's own guard: an unapplied mutation would read `[(10)]`.

DROP TABLE IF EXISTS t_nested_cleared;
CREATE TABLE t_nested_cleared (c0 Nested(c1 Int32, c2 Tuple(c3 Int32)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_cleared (`c0.c1`, `c0.c2`) VALUES ([1], [(10)]);
ALTER TABLE t_nested_cleared CLEAR COLUMN `c0.c2` SETTINGS mutations_sync = 2;

SELECT '-- the fixture: one active Compact part';
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_nested_cleared' AND active;
SELECT '-- the fixture: the cleared member is absent from the part';
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_cleared' AND active
AND column LIKE 'c0.%' ORDER BY column;

SELECT '-- the cleared member and its `size0` subcolumn read together';
SELECT `c0.c2`, `c0.c2`.size0 FROM t_nested_cleared;
SELECT '-- the same pair in the other order';
SELECT `c0.c2`.size0, `c0.c2` FROM t_nested_cleared;
SELECT '-- the subcolumn alone was already correct and must stay so';
SELECT `c0.c2`.size0 FROM t_nested_cleared;

DROP TABLE t_nested_cleared;
