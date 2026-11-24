-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_part_storage_type_wide;
DROP TABLE IF EXISTS t_part_storage_type_compact;

CREATE TABLE t_part_storage_type_wide (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

CREATE TABLE t_part_storage_type_compact (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '100G';

INSERT INTO t_part_storage_type_wide VALUES (1, 'a');
INSERT INTO t_part_storage_type_compact VALUES (1, 'a');

SELECT table, part_type, part_storage_type
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_part_storage_type_wide', 't_part_storage_type_compact') AND active
ORDER BY table;

DROP TABLE t_part_storage_type_wide;
DROP TABLE t_part_storage_type_compact;
