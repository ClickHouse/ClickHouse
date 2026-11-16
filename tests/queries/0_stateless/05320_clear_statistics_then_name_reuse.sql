-- A `CLEAR STATISTICS` names a column by the name it had when the command ran. A later command of the
-- same mutation can give that name to another column, and the clear must still apply to the column it
-- named: the surviving column keeps its statistics, and the cleared one loses them wherever it ends up.

SET materialize_statistics_on_insert = 1;
DROP TABLE IF EXISTS t_clear_statistics_name_reuse;

CREATE TABLE t_clear_statistics_name_reuse (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_clear_statistics_name_reuse VALUES (1, 2, 3);

-- The old `a` is cleared and dropped; the column that takes its name keeps its statistics.
ALTER TABLE t_clear_statistics_name_reuse
    CLEAR STATISTICS a IN PARTITION tuple(), DROP COLUMN a, RENAME COLUMN b TO a SETTINGS mutations_sync = 2;

SELECT 'a clear, a drop and a rename onto the dropped name';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_statistics_name_reuse' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT * FROM t_clear_statistics_name_reuse;

DROP TABLE t_clear_statistics_name_reuse;

CREATE TABLE t_clear_statistics_name_reuse (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_clear_statistics_name_reuse VALUES (1, 2, 3);

-- The cleared column is `a1` afterwards.
ALTER TABLE t_clear_statistics_name_reuse
    CLEAR STATISTICS a IN PARTITION tuple(), RENAME COLUMN a TO a1 SETTINGS mutations_sync = 2;

SELECT 'a clear and a rename of the cleared column';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_statistics_name_reuse' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT * FROM t_clear_statistics_name_reuse;

DROP TABLE t_clear_statistics_name_reuse;

CREATE TABLE t_clear_statistics_name_reuse (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_clear_statistics_name_reuse VALUES (1, 2, 3);

-- A compact part is rewritten in full, which takes a separate path.
-- The old `a` is cleared and dropped; the column that takes its name keeps its statistics.
ALTER TABLE t_clear_statistics_name_reuse
    CLEAR STATISTICS a IN PARTITION tuple(), DROP COLUMN a, RENAME COLUMN b TO a SETTINGS mutations_sync = 2;

SELECT 'compact part: a clear, a drop and a rename onto the dropped name';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_statistics_name_reuse' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT * FROM t_clear_statistics_name_reuse;

DROP TABLE t_clear_statistics_name_reuse;

CREATE TABLE t_clear_statistics_name_reuse (a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_clear_statistics_name_reuse VALUES (1, 2, 3);

-- The cleared column is `a1` afterwards.
ALTER TABLE t_clear_statistics_name_reuse
    CLEAR STATISTICS a IN PARTITION tuple(), RENAME COLUMN a TO a1 SETTINGS mutations_sync = 2;

SELECT 'compact part: a clear and a rename of the cleared column';
SELECT column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_statistics_name_reuse' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT * FROM t_clear_statistics_name_reuse;

DROP TABLE t_clear_statistics_name_reuse;
