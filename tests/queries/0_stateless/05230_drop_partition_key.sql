-- Tags: zookeeper, no-random-merge-tree-settings

-- The forced implicit-projection checks below require insert-time statistics.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_drop_partition_key;

CREATE TABLE t_drop_partition_key
(
    d Date,
    id UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id;

SYSTEM STOP MERGES t_drop_partition_key;

INSERT INTO t_drop_partition_key VALUES ('2024-01-01', 1), ('2024-02-01', 2);

SELECT count() FROM t_drop_partition_key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_partition_key' AND active;

ALTER TABLE t_drop_partition_key DROP PARTITION KEY;

SELECT count() FROM t_drop_partition_key;
SELECT positionCaseInsensitive(create_table_query, 'PARTITION BY') FROM system.tables
WHERE database = currentDatabase() AND name = 't_drop_partition_key';

-- DROP must rebuild the implicit minmax/count projection for an empty partition key instead of
-- keeping the old partition-key projection or removing the implicit projection altogether.
SELECT count() FROM t_drop_partition_key
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;
SELECT min(d) FROM t_drop_partition_key
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;

SELECT _partition_value FROM t_drop_partition_key LIMIT 1; -- { serverError UNKNOWN_IDENTIFIER }

DETACH TABLE t_drop_partition_key;
ATTACH TABLE t_drop_partition_key;

SELECT count() FROM t_drop_partition_key;

INSERT INTO t_drop_partition_key VALUES ('2024-03-01', 3);
SELECT count(), uniqExact(_partition_id) FROM t_drop_partition_key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_partition_key' AND active;

ALTER TABLE t_drop_partition_key DROP PARTITION KEY; -- { serverError BAD_ARGUMENTS }

SYSTEM START MERGES t_drop_partition_key;
DROP TABLE t_drop_partition_key;

-- A rewrite after DROP must preserve enough on-disk state to recognize the legacy
-- partition after restart. Otherwise a merged old part becomes indistinguishable
-- from a corrupted ordinary unpartitioned part.
DROP TABLE IF EXISTS t_drop_partition_key_merged_part;

CREATE TABLE t_drop_partition_key_merged_part
(
    d Date,
    id UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id;

SYSTEM STOP MERGES t_drop_partition_key_merged_part;
INSERT INTO t_drop_partition_key_merged_part VALUES ('2024-01-01', 1);
INSERT INTO t_drop_partition_key_merged_part VALUES ('2024-01-02', 2);
ALTER TABLE t_drop_partition_key_merged_part DROP PARTITION KEY;
SYSTEM START MERGES t_drop_partition_key_merged_part;
OPTIMIZE TABLE t_drop_partition_key_merged_part FINAL;

DETACH TABLE t_drop_partition_key_merged_part;
ATTACH TABLE t_drop_partition_key_merged_part;

SELECT count(), uniqExact(_partition_id), min(_partition_id) FROM t_drop_partition_key_merged_part;
DROP TABLE t_drop_partition_key_merged_part;

CREATE TABLE t_drop_partition_key_no_key (id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_drop_partition_key_no_key DROP PARTITION KEY; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_drop_partition_key_no_key;

-- ReplicatedMergeTree applies ALTER metadata through ReplicatedMergeTreeTableMetadata::Diff.
-- Verify that it compares the new empty key with the real old key and rebuilds the implicit
-- projection with the post-DROP shape.
DROP TABLE IF EXISTS t_drop_partition_key_replicated SYNC;

CREATE TABLE t_drop_partition_key_replicated
(
    d Date,
    id UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05230_drop_partition_key', 'r1')
PARTITION BY toYYYYMM(d)
ORDER BY id;

INSERT INTO t_drop_partition_key_replicated VALUES ('2024-01-01', 1), ('2024-02-01', 2);
ALTER TABLE t_drop_partition_key_replicated DROP PARTITION KEY;

SELECT count() FROM t_drop_partition_key_replicated
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;
SELECT min(d) FROM t_drop_partition_key_replicated
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;

DETACH TABLE t_drop_partition_key_replicated;
ATTACH TABLE t_drop_partition_key_replicated;

SELECT count() FROM t_drop_partition_key_replicated;
DROP TABLE t_drop_partition_key_replicated SYNC;
