-- A part created before the target column was added has no data stream to recompress. In particular,
-- a compact part must not fall back to a whole-part rewrite and apply a queued codec change to `a`.
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_recompress_missing_part_column;

CREATE TABLE t_recompress_missing_part_column (a String CODEC(NONE))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_recompress_missing_part_column SELECT repeat('a', 100) FROM numbers(1000);

ALTER TABLE t_recompress_missing_part_column ADD COLUMN b UInt64;
ALTER TABLE t_recompress_missing_part_column MODIFY COLUMN a String CODEC(ZSTD);
ALTER TABLE t_recompress_missing_part_column RECOMPRESS COLUMN b;

SELECT 'old compact part was not rewritten', sum(data_compressed_bytes) > 90000
FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_missing_part_column' AND active;

SELECT 'values are intact', count(), countIf(a = repeat('a', 100)), countIf(b = 0)
FROM t_recompress_missing_part_column;

DROP TABLE t_recompress_missing_part_column;
