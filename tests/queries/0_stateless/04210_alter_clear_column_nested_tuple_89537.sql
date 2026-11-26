-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/89537
--
-- `ALTER TABLE ... CLEAR COLUMN` on a `Tuple`-typed subcolumn of a `Nested`
-- column followed by a merge (e.g. `OPTIMIZE TABLE ... FINAL`) used to abort
-- with `Logical error: 'Bad cast from type DB::ColumnVector<int> to DB::ColumnTuple'`
-- inside `SerializationTuple::serializeBinaryBulkStatePrefix` when the
-- merge writer tried to serialize the cleared subcolumn.
--
-- The expected behaviour is that the operation succeeds: the inner `Tuple`
-- elements are reset to their default values, but the outer `Nested` array
-- length stays aligned with the sibling `Nested` arrays.

DROP TABLE IF EXISTS t_nested_clear_89537;

-- Original repro from the issue: a single row with a one-element nested array.
CREATE TABLE t_nested_clear_89537 (c0 Nested(c1 Int, c2 Tuple(c3 Int)))
ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t_nested_clear_89537 (`c0.c1`, `c0.c2`) VALUES ([1], [(1)]);

ALTER TABLE t_nested_clear_89537 CLEAR COLUMN `c0.c2`;

OPTIMIZE TABLE t_nested_clear_89537 FINAL;

SELECT 'original repro';
SELECT `c0.c1`, `c0.c2`, length(`c0.c1`) AS len1, length(`c0.c2`) AS len2
FROM t_nested_clear_89537;

DROP TABLE t_nested_clear_89537;

-- Multi-row, multi-part scenario: every part must be merge-able after the
-- cleared subcolumn is overwritten with default-typed columns. Hardens the
-- regression check against partial (per-part) reintroductions of the bug.
CREATE TABLE t_nested_clear_89537 (c0 Nested(c1 Int, c2 Tuple(c3 Int)))
ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t_nested_clear_89537 (`c0.c1`, `c0.c2`) VALUES ([1], [(10)]);
INSERT INTO TABLE t_nested_clear_89537 (`c0.c1`, `c0.c2`) VALUES ([2, 3], [(20), (30)]);
INSERT INTO TABLE t_nested_clear_89537 (`c0.c1`, `c0.c2`) VALUES ([], []);

ALTER TABLE t_nested_clear_89537 CLEAR COLUMN `c0.c2`;

OPTIMIZE TABLE t_nested_clear_89537 FINAL;

SELECT 'multi-part';
SELECT `c0.c1`, `c0.c2`, length(`c0.c1`) AS len1, length(`c0.c2`) AS len2
FROM t_nested_clear_89537 ORDER BY `c0.c1`;

-- `INSERT` after `CLEAR` must continue to accept fresh non-default tuple
-- values, and a follow-up merge must still succeed.
INSERT INTO TABLE t_nested_clear_89537 (`c0.c1`, `c0.c2`) VALUES ([4, 5], [(40), (50)]);

OPTIMIZE TABLE t_nested_clear_89537 FINAL;

SELECT 'insert-after-clear';
SELECT `c0.c1`, `c0.c2`, length(`c0.c1`) AS len1, length(`c0.c2`) AS len2
FROM t_nested_clear_89537 ORDER BY `c0.c1`;

DROP TABLE t_nested_clear_89537;
