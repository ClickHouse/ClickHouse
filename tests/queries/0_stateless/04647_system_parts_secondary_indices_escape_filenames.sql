-- Tests that system.parts.secondary_indices_materialized follows the table's current
-- `escape_index_filenames` policy: an index file written under the other policy is not
-- usable by the read path, so it is reported as not materialized until it is rewritten
-- with `ALTER TABLE ... MATERIALIZE INDEX`.
-- The two policies only differ for index names that need escaping in a file name.

DROP TABLE IF EXISTS t_secondary_indices_escape;

CREATE TABLE t_secondary_indices_escape
(
    a UInt64,
    INDEX `idx_ESPAÑA` a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, escape_index_filenames = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_secondary_indices_escape SELECT number FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

SELECT 'escaped filenames';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_escape' AND active;

-- The part keeps the escaped file name, which the unescaped policy cannot read.
ALTER TABLE t_secondary_indices_escape MODIFY SETTING escape_index_filenames = 0;

SELECT 'after flipping the policy';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_escape' AND active;

-- Rewriting the index under the new policy makes it materialized again.
ALTER TABLE t_secondary_indices_escape MATERIALIZE INDEX `idx_ESPAÑA` SETTINGS mutations_sync = 2;

SELECT 'after materializing under the new policy';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_escape' AND active;

DROP TABLE t_secondary_indices_escape;
