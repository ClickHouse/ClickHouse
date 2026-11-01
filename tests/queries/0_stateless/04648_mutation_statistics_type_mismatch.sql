-- Tags: no-random-merge-tree-settings

SET mutations_sync = 2;
-- The statistics object must exist in the part before the metadata-only conversion, otherwise
-- nothing is loaded during the mutation and the test would not exercise anything.
SET materialize_statistics_on_insert = 1;

-- A mutation that rewrites a column writes it at the column's current metadata type, while the
-- statistics object is loaded from the source part at the part's own (older) type. Enum8 -> Int8 is
-- a metadata-only conversion, so it schedules no mutation and leaves the part at Enum8.

SELECT '-- MATERIALIZE COLUMN';

DROP TABLE IF EXISTS t_stats_materialize_column;
CREATE TABLE t_stats_materialize_column (id Int64, e Enum8('a' = 1, 'b' = 2) MATERIALIZED 'a' STATISTICS(uniq, basic))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_materialize_column (id) VALUES (1), (2);
SELECT part_type, type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_stats_materialize_column' AND active AND column = 'e';

ALTER TABLE t_stats_materialize_column MODIFY COLUMN e Int8 MATERIALIZED 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_stats_materialize_column';
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_stats_materialize_column' AND active AND column = 'e';

ALTER TABLE t_stats_materialize_column MATERIALIZE COLUMN e;
SELECT id, e FROM t_stats_materialize_column ORDER BY id;
-- The rewritten column's statistics are recomputed at the new type, so they are usable again.
SELECT `estimates.cardinality`, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_materialize_column' AND active AND column = 'e';

DROP TABLE t_stats_materialize_column;

SELECT '-- UPDATE recalculating a dependent MATERIALIZED column';

DROP TABLE IF EXISTS t_stats_affected_materialized;
CREATE TABLE t_stats_affected_materialized (id Int64, src Int8, m Enum8('a' = 1, 'b' = 2) MATERIALIZED CAST(src, 'Enum8(\'a\' = 1, \'b\' = 2)') STATISTICS(uniq, basic))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_affected_materialized (id, src) VALUES (1, 1), (2, 2);
ALTER TABLE t_stats_affected_materialized MODIFY COLUMN m Int8 MATERIALIZED src;
ALTER TABLE t_stats_affected_materialized UPDATE src = 2 WHERE id = 1;
SELECT id, src, m FROM t_stats_affected_materialized ORDER BY id;
SELECT `estimates.cardinality`, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_affected_materialized' AND active AND column = 'm';

DROP TABLE t_stats_affected_materialized;

SELECT '-- CLEAR COLUMN recalculating MATERIALIZED columns';

-- CLEAR only recomputes MATERIALIZED columns derived from c. The metadata-only
-- change of unrelated e remains unapplied to existing rows and its statistics
-- stay aligned with the stored value.
DROP TABLE IF EXISTS t_stats_clear_column;
CREATE TABLE t_stats_clear_column (id Int64, c Int64, e Enum8('a' = 1, 'b' = 2) MATERIALIZED 'a' STATISTICS(uniq, basic), d Int64 MATERIALIZED c + 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_clear_column (id, c) VALUES (1, 10), (2, 20);
ALTER TABLE t_stats_clear_column MODIFY COLUMN e Int8 MATERIALIZED 2;
ALTER TABLE t_stats_clear_column CLEAR COLUMN c;
SELECT id, c, e, d FROM t_stats_clear_column ORDER BY id;
SELECT `estimates.cardinality`, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_clear_column' AND active AND column = 'e';

DROP TABLE t_stats_clear_column;

SELECT '-- CLEAR COLUMN of the column carrying statistics';

-- A cleared column is dropped from the new part together with its statistics, and the remaining
-- columns keep usable statistics.
DROP TABLE IF EXISTS t_stats_cleared_target;
CREATE TABLE t_stats_cleared_target (id Int64, c Int64 STATISTICS(uniq, basic), v Int64 MATERIALIZED c * 2 STATISTICS(uniq, basic))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_cleared_target (id, c) SELECT number, number % 7 FROM numbers(100);
ALTER TABLE t_stats_cleared_target CLEAR COLUMN v;
SELECT sum(v), sum(c) FROM t_stats_cleared_target;
SELECT column, `estimates.cardinality` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_cleared_target' AND active AND column IN ('c', 'v') ORDER BY column;

DROP TABLE t_stats_cleared_target;

SELECT '-- MATERIALIZE INDEX carries statistics of columns it does not rewrite';

-- A pure index materialization writes no data column, so the new part keeps the source column type
-- and its statistics must be carried over untouched instead of being rebuilt from the block.
DROP TABLE IF EXISTS t_stats_materialize_index;
CREATE TABLE t_stats_materialize_index (id Int64, e Enum8('a' = 1, 'b' = 2) MATERIALIZED 'a' STATISTICS(uniq, basic))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_materialize_index (id) VALUES (1), (2);
ALTER TABLE t_stats_materialize_index MODIFY COLUMN e Int8 MATERIALIZED 2;
ALTER TABLE t_stats_materialize_index ADD INDEX idx_e e TYPE minmax GRANULARITY 1;
ALTER TABLE t_stats_materialize_index MATERIALIZE INDEX idx_e;
SELECT id, e FROM t_stats_materialize_index ORDER BY id;
-- The column keeps the source part's type, and its carried statistics are still readable.
SELECT type, `estimates.cardinality` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_materialize_index' AND active AND column = 'e';

DROP TABLE t_stats_materialize_index;

SELECT '-- statistics stay prunable after MATERIALIZE STATISTICS and after DELETE';

DROP TABLE IF EXISTS t_stats_not_emptied;
CREATE TABLE t_stats_not_emptied (id Int64, v Int64 STATISTICS(uniq, basic))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO t_stats_not_emptied SELECT number, number % 7 FROM numbers(1000);

ALTER TABLE t_stats_not_emptied MATERIALIZE STATISTICS v;
SELECT `estimates.cardinality`, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_not_emptied' AND active AND column = 'v';

ALTER TABLE t_stats_not_emptied DELETE WHERE id = 999;
SELECT `estimates.cardinality`, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_not_emptied' AND active AND column = 'v';

DROP TABLE t_stats_not_emptied;
