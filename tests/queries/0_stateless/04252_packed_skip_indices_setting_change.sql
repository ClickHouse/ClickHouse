-- packed_skip_index_types takes effect at write time: existing parts keep whatever layout
-- they had when they were written, and future INSERTs / merges apply the current setting.
-- Verify both directions (off→on, on→off) preserve readability of the old part and adopt
-- the new layout for new parts. Distinguish per-file vs packed layouts via system.parts.files
-- (per-file layout produces one more on-disk file than packed for a single-substream index).
--
-- Test both Compact and Wide part layouts.

-- ------------------------------------------------------------------
-- Wide: setting goes from "" to "minmax".
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_off_to_on;
CREATE TABLE t_wide_off_to_on
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = '', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_wide_off_to_on SELECT number, number * 7 FROM numbers(10000);
SELECT 'wide_off2on_files_before', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active;

ALTER TABLE t_wide_off_to_on MODIFY SETTING packed_skip_index_types = 'minmax';

-- Old part still queries correctly with the new setting in metadata.
SELECT 'wide_off2on_query_old_part', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;

-- New INSERT picks up the new setting.
INSERT INTO t_wide_off_to_on SELECT number + 10000, number * 11 FROM numbers(10000);

-- The two parts must differ in file count: old one has per-file substreams, new one packed.
SELECT 'wide_off2on_files_after_insert', name, files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active
ORDER BY name;
SELECT 'wide_off2on_combined_count', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;

-- Merge: the resulting single part uses the current writer setting, i.e. packed.
OPTIMIZE TABLE t_wide_off_to_on FINAL;
SELECT 'wide_off2on_files_after_merge', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active;
SELECT 'wide_off2on_count_after_merge', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_wide_off_to_on SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wide_off_to_on;

-- ------------------------------------------------------------------
-- Wide: setting goes from "minmax" to "".
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_on_to_off;
CREATE TABLE t_wide_on_to_off
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_wide_on_to_off SELECT number, number * 7 FROM numbers(10000);
SELECT 'wide_on2off_files_before', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active;

ALTER TABLE t_wide_on_to_off MODIFY SETTING packed_skip_index_types = '';

SELECT 'wide_on2off_query_old_part', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;

INSERT INTO t_wide_on_to_off SELECT number + 10000, number * 11 FROM numbers(10000);

SELECT 'wide_on2off_files_after_insert', name, files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active
ORDER BY name;
SELECT 'wide_on2off_combined_count', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_wide_on_to_off FINAL;
SELECT 'wide_on2off_files_after_merge', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active;
SELECT 'wide_on2off_count_after_merge', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_wide_on_to_off SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wide_on_to_off;

-- ------------------------------------------------------------------
-- Compact: setting goes from "" to "minmax".
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_compact_off_to_on;
CREATE TABLE t_compact_off_to_on
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_types = '', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_compact_off_to_on SELECT number, number * 7 FROM numbers(10000);
SELECT 'compact_off2on_files_before', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active;

ALTER TABLE t_compact_off_to_on MODIFY SETTING packed_skip_index_types = 'minmax';

SELECT 'compact_off2on_query_old_part', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;

INSERT INTO t_compact_off_to_on SELECT number + 10000, number * 11 FROM numbers(10000);

SELECT 'compact_off2on_files_after_insert', name, files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active
ORDER BY name;
SELECT 'compact_off2on_combined_count', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_compact_off_to_on FINAL;
SELECT 'compact_off2on_files_after_merge', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active;
SELECT 'compact_off2on_count_after_merge', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_compact_off_to_on SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_off_to_on;

-- ------------------------------------------------------------------
-- Compact: setting goes from "minmax" to "".
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_compact_on_to_off;
CREATE TABLE t_compact_on_to_off
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_types = 'minmax', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_compact_on_to_off SELECT number, number * 7 FROM numbers(10000);
SELECT 'compact_on2off_files_before', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active;

ALTER TABLE t_compact_on_to_off MODIFY SETTING packed_skip_index_types = '';

SELECT 'compact_on2off_query_old_part', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;

INSERT INTO t_compact_on_to_off SELECT number + 10000, number * 11 FROM numbers(10000);

SELECT 'compact_on2off_files_after_insert', name, files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active
ORDER BY name;
SELECT 'compact_on2off_combined_count', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_compact_on_to_off FINAL;
SELECT 'compact_on2off_files_after_merge', files FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active;
SELECT 'compact_on2off_count_after_merge', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_compact_on_to_off SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_on_to_off;

-- ------------------------------------------------------------------
-- Regression: setting flip + ALTER UPDATE on indexed column.
-- The transition writer ignores the source layout and uses the new setting:
-- per-file -> packed must drop the stale per-substream checksum entries,
-- packed -> per-file must drop the stale archive checksum entry. CHECK TABLE
-- catches either if they linger.
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_flip_mutate_off2on;
CREATE TABLE t_wide_flip_mutate_off2on
(
    id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = '', auto_statistics_types = '', index_granularity = 1024;
INSERT INTO t_wide_flip_mutate_off2on SELECT number, number * 7 FROM numbers(10000);
ALTER TABLE t_wide_flip_mutate_off2on MODIFY SETTING packed_skip_index_types = 'minmax';
ALTER TABLE t_wide_flip_mutate_off2on UPDATE v = v + 1 WHERE id < 100 SETTINGS mutations_sync = 2;
SELECT 'wide_flip_off2on_count', count() FROM t_wide_flip_mutate_off2on WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_wide_flip_mutate_off2on SETTINGS check_query_single_value_result = 1;
DROP TABLE t_wide_flip_mutate_off2on;

DROP TABLE IF EXISTS t_wide_flip_mutate_on2off;
CREATE TABLE t_wide_flip_mutate_on2off
(
    id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', auto_statistics_types = '', index_granularity = 1024;
INSERT INTO t_wide_flip_mutate_on2off SELECT number, number * 7 FROM numbers(10000);
ALTER TABLE t_wide_flip_mutate_on2off MODIFY SETTING packed_skip_index_types = '';
ALTER TABLE t_wide_flip_mutate_on2off UPDATE v = v + 1 WHERE id < 100 SETTINGS mutations_sync = 2;
SELECT 'wide_flip_on2off_count', count() FROM t_wide_flip_mutate_on2off WHERE v BETWEEN 70 AND 700;
CHECK TABLE t_wide_flip_mutate_on2off SETTINGS check_query_single_value_result = 1;
DROP TABLE t_wide_flip_mutate_on2off;
