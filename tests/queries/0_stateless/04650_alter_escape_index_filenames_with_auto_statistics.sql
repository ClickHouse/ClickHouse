-- Tests that `ALTER TABLE ... MODIFY SETTING escape_index_filenames` takes effect immediately even when
-- the table also carries an explicit `auto_statistics_types` setting. Such an `ALTER` re-commits the
-- implicit statistics, and that commit used to revert the index filename policy in the in-memory
-- metadata, so the new policy only became visible after a server restart.

DROP TABLE IF EXISTS t_escape_index_filenames_stats;

CREATE TABLE t_escape_index_filenames_stats
(
    a UInt64,
    INDEX `idx_ESPAÑA` a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, escape_index_filenames = 1, add_minmax_index_for_numeric_columns = 0,
         auto_statistics_types = 'basic';

INSERT INTO t_escape_index_filenames_stats SELECT number FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

SELECT 'escaped filenames';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_index_filenames_stats' AND active;

-- The index file keeps its escaped name, which the unescaped policy cannot read.
ALTER TABLE t_escape_index_filenames_stats MODIFY SETTING escape_index_filenames = 0;

SELECT 'after flipping the policy';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_index_filenames_stats' AND active;

-- The same `ALTER` with the statistics also re-committed must not revert the policy either.
ALTER TABLE t_escape_index_filenames_stats MODIFY SETTING escape_index_filenames = 0, auto_statistics_types = 'basic, uniq_v2';

SELECT 'after re-committing the statistics';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_index_filenames_stats' AND active;

DROP TABLE t_escape_index_filenames_stats;
