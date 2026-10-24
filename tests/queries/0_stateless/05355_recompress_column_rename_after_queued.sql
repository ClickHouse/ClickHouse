-- Tags: no-replicated-database
-- no-replicated-database: combines `RENAME COLUMN` and `RECOMPRESS COLUMN` in a single query, which is
-- prohibited for Replicated databases

-- `ALTER TABLE ... RECOMPRESS COLUMN a` that is still queued, followed by `RENAME COLUMN a TO b`:
-- the queued command names the column by its old name, which would no longer be a physical column
-- of the table metadata when the mutation runs, so the recompression would be skipped and the
-- renamed column would keep its old codec. The rename must therefore be rejected until the
-- recompression is finished. Both commands in a single `ALTER` statement are fine.

DROP TABLE IF EXISTS t_recompress_rename_after_queued;

CREATE TABLE t_recompress_rename_after_queued (id UInt64, a String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_rename_after_queued SELECT number, repeat('a', 200) FROM numbers(10000);
ALTER TABLE t_recompress_rename_after_queued MODIFY COLUMN a String CODEC(ZSTD(3));

SYSTEM STOP MERGES t_recompress_rename_after_queued;
ALTER TABLE t_recompress_rename_after_queued RECOMPRESS COLUMN a SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_rename_after_queued RENAME COLUMN a TO b; -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_recompress_rename_after_queued;

DROP TABLE t_recompress_rename_after_queued;

CREATE TABLE t_recompress_rename_after_queued (id UInt64, a String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_rename_after_queued SELECT number, repeat('a', 200) FROM numbers(10000);
ALTER TABLE t_recompress_rename_after_queued MODIFY COLUMN a String CODEC(ZSTD(3));
ALTER TABLE t_recompress_rename_after_queued RECOMPRESS COLUMN a, RENAME COLUMN a TO b SETTINGS mutations_sync = 2;

-- 10000 rows of repeat('a', 200) are ~2 MB under `CODEC(NONE)` and ~50 KB under `ZSTD(3)`.
SELECT column, sum(data_compressed_bytes) < 500000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_rename_after_queued' AND active AND column IN ('a', 'b')
GROUP BY column;
SELECT 'data intact', count() FROM t_recompress_rename_after_queued WHERE b = repeat('a', 200);

DROP TABLE t_recompress_rename_after_queued;
