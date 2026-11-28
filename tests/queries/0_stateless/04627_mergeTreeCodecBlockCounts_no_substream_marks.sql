-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Wide.

-- A Compact part written with write_marks_for_substreams_in_compact_parts=0 records its columns_substreams too,
-- so mergeTreeCodecBlockCounts lists it like any other Compact part.

DROP TABLE IF EXISTS t_no_substream_marks;

CREATE TABLE t_no_substream_marks (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO t_no_substream_marks SELECT number, number FROM numbers(1000);

SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_no_substream_marks' AND active;

-- One row per substream: `a` and `b`.
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_no_substream_marks);

DROP TABLE t_no_substream_marks;
