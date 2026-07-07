DROP TABLE IF EXISTS t_system_columns_physical_presence;
SET enable_lightweight_update = 1;

CREATE TABLE t_system_columns_physical_presence
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    enable_block_number_column = true,
    enable_block_offset_column = true,
    remove_unused_patch_parts = false;

SYSTEM STOP MERGES t_system_columns_physical_presence;

SELECT
    name,
    parts_with_column_num,
    round(parts_with_column_ratio, 3),
    rows_with_column_num,
    round(rows_with_column_ratio, 3)
FROM system.columns
WHERE database = currentDatabase() AND table = 't_system_columns_physical_presence'
ORDER BY name;

INSERT INTO t_system_columns_physical_presence SELECT number FROM numbers(10);
ALTER TABLE t_system_columns_physical_presence ADD COLUMN b UInt64 DEFAULT 0;
INSERT INTO t_system_columns_physical_presence (a, b) SELECT number, number FROM numbers(30);

SELECT
    name,
    parts_with_column_num,
    round(parts_with_column_ratio, 3),
    rows_with_column_num,
    round(rows_with_column_ratio, 3)
FROM system.columns
WHERE database = currentDatabase() AND table = 't_system_columns_physical_presence'
ORDER BY name;

UPDATE t_system_columns_physical_presence SET b = b + 1 WHERE a = 0;

SELECT
    name,
    parts_with_column_num,
    round(parts_with_column_ratio, 3),
    rows_with_column_num,
    round(rows_with_column_ratio, 3)
FROM system.columns
WHERE database = currentDatabase() AND table = 't_system_columns_physical_presence'
ORDER BY name;

DROP TABLE t_system_columns_physical_presence;
