-- Tags: no-fasttest

-- A packed part keeps its logical files (including per-column statistics) inside a single data.packed
-- archive; there is no real filesystem entry per logical file. When a column has a very long name and
-- replace_long_file_name_to_hash is off, the logical statistics file name exceeds the OS filename
-- limit. Loading the part runs checkConsistency, which must not probe the real filesystem for such a
-- logical file: probing a too-long path throws ENAMETOOLONG and would wrongly mark the packed part
-- broken. The write itself succeeds because the file only lives inside the archive.

SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_packed_long_stat SYNC;

CREATE TABLE t_packed_long_stat
(
    id UInt64,
    `col_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` Int64 STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0;

INSERT INTO t_packed_long_stat SELECT number, number FROM numbers(100);

SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_long_stat' AND active;

-- Force a reload from disk, which runs checkConsistency on the packed part.
DETACH TABLE t_packed_long_stat;
ATTACH TABLE t_packed_long_stat;

SELECT count() FROM t_packed_long_stat;

DROP TABLE t_packed_long_stat SYNC;
