-- Implicit minmax indices must follow `MODIFY COLUMN` changes of the default kind:
-- a column turning EPHEMERAL loses its implicit index and a column turning physical gains one.
-- Previously the first ALTER below failed with
-- "Cannot apply ALTER because it breaks skip index auto_minmax_index_type_uid".
-- The assertions filter by the exact index name to stay independent of implicit indices
-- that randomized merge tree settings may add for other columns.

-- Physical -> EPHEMERAL with a type change.
CREATE TABLE t_phys_to_eph (event String, type_uid UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_phys_to_eph VALUES ('{"type_uid":1}', 1);

SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_phys_to_eph' AND name = 'auto_minmax_index_type_uid';
ALTER TABLE t_phys_to_eph MODIFY COLUMN type_uid UInt32 EPHEMERAL 1 CODEC(T64, LZ4);
SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_phys_to_eph' AND name = 'auto_minmax_index_type_uid';
SELECT event FROM t_phys_to_eph;

-- Physical -> EPHEMERAL without a type change: the stale implicit index must be removed too.
CREATE TABLE t_phys_to_eph_no_type (event String, type_uid UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

ALTER TABLE t_phys_to_eph_no_type MODIFY COLUMN type_uid EPHEMERAL 1;
SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_phys_to_eph_no_type' AND name = 'auto_minmax_index_type_uid';

-- MATERIALIZED with a codec -> EPHEMERAL.
CREATE TABLE t_mat_to_eph (event String, type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mat_to_eph' AND name = 'auto_minmax_index_type_uid';
ALTER TABLE t_mat_to_eph MODIFY COLUMN type_uid UInt32 EPHEMERAL 1;
SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mat_to_eph' AND name = 'auto_minmax_index_type_uid';

-- EPHEMERAL -> physical gains the implicit index.
CREATE TABLE t_eph_to_phys (event String, type_uid UInt32 EPHEMERAL 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_eph_to_phys' AND name = 'auto_minmax_index_type_uid';
ALTER TABLE t_eph_to_phys MODIFY COLUMN type_uid UInt32 DEFAULT 1;
SELECT count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_eph_to_phys' AND name = 'auto_minmax_index_type_uid';

DROP TABLE t_phys_to_eph;
DROP TABLE t_phys_to_eph_no_type;
DROP TABLE t_mat_to_eph;
DROP TABLE t_eph_to_phys;
