-- Tests the system.parts.secondary_indices_materialized column, which lists the
-- names of the secondary (data skipping) indices materialized in each data part.

DROP TABLE IF EXISTS t_secondary_indices_materialized;

CREATE TABLE t_secondary_indices_materialized
(
    a UInt64,
    b UInt64,
    c String,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_secondary_indices_materialized SELECT number, number * 2, toString(number) FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

-- Both declared indices are materialized in the freshly written part.
SELECT 'after insert';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_materialized' AND active;

-- A newly added index is not materialized in the existing part yet.
ALTER TABLE t_secondary_indices_materialized ADD INDEX idx_c c TYPE bloom_filter GRANULARITY 1;

SELECT 'after add index';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_materialized' AND active;

-- After materializing the index it appears for the rewritten part.
ALTER TABLE t_secondary_indices_materialized MATERIALIZE INDEX idx_c SETTINGS mutations_sync = 2;

SELECT 'after materialize index';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_secondary_indices_materialized' AND active;

-- A table without any secondary indices reports an empty array.
DROP TABLE IF EXISTS t_no_secondary_indices;

CREATE TABLE t_no_secondary_indices (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_no_secondary_indices SELECT number FROM numbers(10);

SELECT 'no secondary indices';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_no_secondary_indices' AND active;

DROP TABLE t_secondary_indices_materialized;
DROP TABLE t_no_secondary_indices;
