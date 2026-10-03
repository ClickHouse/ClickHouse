-- Tags: zookeeper, no-random-merge-tree-settings
-- A settings ALTER that also changes the implicit statistics publishes the interpreter-built metadata,
-- which must not revert the index filename escaping switched by the same ALTER.

DROP TABLE IF EXISTS t_escape_settings_alter_replicated SYNC;

CREATE TABLE t_escape_settings_alter_replicated (k UInt64, v UInt64, INDEX `a-b` v TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_escape_settings_alter_replicated', 'r1') ORDER BY k
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 0, escape_index_filenames = 1;

ALTER TABLE t_escape_settings_alter_replicated MODIFY SETTING escape_index_filenames = 0, auto_statistics_types = 'basic';

INSERT INTO t_escape_settings_alter_replicated SELECT number, number FROM numbers(500);

-- The part is written under the published metadata, the reload reads it under the stored setting.
DETACH TABLE t_escape_settings_alter_replicated;
ATTACH TABLE t_escape_settings_alter_replicated;

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_escape_settings_alter_replicated WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%';
SELECT count() FROM t_escape_settings_alter_replicated WHERE v = 42;
CHECK TABLE t_escape_settings_alter_replicated SETTINGS check_query_single_value_result = 1;

DROP TABLE t_escape_settings_alter_replicated SYNC;

-- The same for a comment change mixed with the settings change, which takes a different path than a pure settings ALTER.
DROP TABLE IF EXISTS t_escape_mixed_alter_replicated SYNC;

CREATE TABLE t_escape_mixed_alter_replicated (k UInt64, v UInt64, INDEX `a-b` v TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_escape_mixed_alter_replicated', 'r1') ORDER BY k
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 0, escape_index_filenames = 1;

ALTER TABLE t_escape_mixed_alter_replicated MODIFY COMMENT 'c', MODIFY SETTING escape_index_filenames = 0;

INSERT INTO t_escape_mixed_alter_replicated SELECT number, number FROM numbers(500);

DETACH TABLE t_escape_mixed_alter_replicated;
ATTACH TABLE t_escape_mixed_alter_replicated;

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_escape_mixed_alter_replicated WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_escape_mixed_alter_replicated';
CHECK TABLE t_escape_mixed_alter_replicated SETTINGS check_query_single_value_result = 1;

DROP TABLE t_escape_mixed_alter_replicated SYNC;
