-- Tags: no-object-storage
-- no-object-storage: the move TTL needs the disk `default`.
-- A merge over a column that no source part stores applies TTL only when allowed (#121959, #121961); `*_control` tables lack that column.

SET optimize_throw_if_noop = 1;
SYSTEM FLUSH LOGS part_log;

-- One row per active part: a successful OPTIMIZE leaves one row, with the algorithm of the merge that wrote it.
-- After `START TTL MERGES` a background TTL merge may race the OPTIMIZE, so only the rows are checked.
CREATE VIEW merged_parts AS
SELECT p.table AS table, p.rows AS rows, l.merge_algorithm AS merge_algorithm
FROM system.parts AS p
LEFT JOIN (SELECT table, part_name, merge_algorithm FROM system.part_log WHERE database = currentDatabase() AND event_type = 'MergeParts') AS l
    ON p.table = l.table AND p.name = l.part_name
WHERE p.database = currentDatabase() AND p.active;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps the background pool off regular merges, so each `OPTIMIZE` does the merge.

-- Rows TTL, horizontal merge, TTL merges stopped: the expired rows survive and the projection counts the same rows.
CREATE TABLE rows_ttl_horizontal (id UInt64, d DateTime, c1 UInt64, c2 UInt64, c3 UInt64, PROJECTION p (SELECT c1, count() GROUP BY c1))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 0;
CREATE TABLE rows_ttl_horizontal_control AS rows_ttl_horizontal;
SYSTEM STOP TTL MERGES rows_ttl_horizontal;
SYSTEM STOP TTL MERGES rows_ttl_horizontal_control;
INSERT INTO rows_ttl_horizontal SELECT number, if(number < 5, '2000-01-01', '2100-01-01'), 1, 1, 1 FROM numbers(10);
INSERT INTO rows_ttl_horizontal SELECT number + 10, if(number < 5, '2000-01-01', '2100-01-01'), 2, 2, 2 FROM numbers(10);
INSERT INTO rows_ttl_horizontal_control SELECT * FROM rows_ttl_horizontal WHERE id < 10;
INSERT INTO rows_ttl_horizontal_control SELECT * FROM rows_ttl_horizontal WHERE id >= 10;
ALTER TABLE rows_ttl_horizontal ADD COLUMN x UInt64;
OPTIMIZE TABLE rows_ttl_horizontal FINAL;
OPTIMIZE TABLE rows_ttl_horizontal_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'rows_ttl_horizontal%' ORDER BY table;
SELECT 'table', c1, count() FROM rows_ttl_horizontal GROUP BY c1 ORDER BY c1 SETTINGS optimize_use_projections = 0;
SELECT 'projection', c1, count() FROM rows_ttl_horizontal GROUP BY c1 ORDER BY c1 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SYSTEM START TTL MERGES rows_ttl_horizontal;
SYSTEM START TTL MERGES rows_ttl_horizontal_control;
OPTIMIZE TABLE rows_ttl_horizontal FINAL;
OPTIMIZE TABLE rows_ttl_horizontal_control FINAL;
SELECT table, rows FROM merged_parts WHERE table LIKE 'rows_ttl_horizontal%' ORDER BY table;
SELECT 'table', c1, count() FROM rows_ttl_horizontal GROUP BY c1 ORDER BY c1 SETTINGS optimize_use_projections = 0;
SELECT 'projection', c1, count() FROM rows_ttl_horizontal GROUP BY c1 ORDER BY c1 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- Rows TTL expired for every row, vertical merge, TTL merges stopped: the expired rows survive.
CREATE TABLE rows_ttl_vertical (id UInt64, d DateTime, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_optimize_ttl_delete = 1;
CREATE TABLE rows_ttl_vertical_control AS rows_ttl_vertical;
SYSTEM STOP TTL MERGES rows_ttl_vertical;
SYSTEM STOP TTL MERGES rows_ttl_vertical_control;
INSERT INTO rows_ttl_vertical SELECT number, '2000-01-01', 1, 1, 1 FROM numbers(10);
INSERT INTO rows_ttl_vertical SELECT number + 10, '2000-01-01', 2, 2, 2 FROM numbers(10);
INSERT INTO rows_ttl_vertical_control SELECT * FROM rows_ttl_vertical WHERE id < 10;
INSERT INTO rows_ttl_vertical_control SELECT * FROM rows_ttl_vertical WHERE id >= 10;
ALTER TABLE rows_ttl_vertical ADD COLUMN x UInt64;
ALTER TABLE rows_ttl_vertical ADD INDEX idx (x, id) TYPE minmax GRANULARITY 1;
OPTIMIZE TABLE rows_ttl_vertical FINAL;
OPTIMIZE TABLE rows_ttl_vertical_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'rows_ttl_vertical%' ORDER BY table;
SYSTEM START TTL MERGES rows_ttl_vertical;
SYSTEM START TTL MERGES rows_ttl_vertical_control;
-- A background TTL merge may drop the fully expired part first and leave nothing to optimize.
OPTIMIZE TABLE rows_ttl_vertical FINAL SETTINGS optimize_throw_if_noop = 0;
OPTIMIZE TABLE rows_ttl_vertical_control FINAL SETTINGS optimize_throw_if_noop = 0;
SELECT 'rows_ttl_vertical', count() FROM rows_ttl_vertical;
SELECT 'rows_ttl_vertical_control', count() FROM rows_ttl_vertical_control;

-- Move and recompression TTL, not due, vertical merge.
CREATE TABLE move_ttl (id UInt64, f DateTime, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY id TTL f + INTERVAL 10 YEAR TO DISK 'default'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
CREATE TABLE move_ttl_control AS move_ttl;
CREATE TABLE recompression_ttl (id UInt64, f DateTime, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY id TTL f + INTERVAL 10 YEAR RECOMPRESS CODEC(ZSTD(3))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
CREATE TABLE recompression_ttl_control AS recompression_ttl;
INSERT INTO move_ttl SELECT number, '2100-01-01', 1, 1, 1 FROM numbers(10);
INSERT INTO move_ttl SELECT number + 10, '2100-01-01', 2, 2, 2 FROM numbers(10);
INSERT INTO move_ttl_control SELECT * FROM move_ttl WHERE id < 10;
INSERT INTO move_ttl_control SELECT * FROM move_ttl WHERE id >= 10;
INSERT INTO recompression_ttl SELECT number, '2100-01-01', 1, 1, 1 FROM numbers(10);
INSERT INTO recompression_ttl SELECT number + 10, '2100-01-01', 2, 2, 2 FROM numbers(10);
INSERT INTO recompression_ttl_control SELECT * FROM recompression_ttl WHERE id < 10;
INSERT INTO recompression_ttl_control SELECT * FROM recompression_ttl WHERE id >= 10;
ALTER TABLE move_ttl ADD COLUMN x UInt64;
ALTER TABLE move_ttl ADD INDEX idx (x, id) TYPE minmax GRANULARITY 1;
ALTER TABLE recompression_ttl ADD COLUMN x UInt64;
ALTER TABLE recompression_ttl ADD INDEX idx (x, id) TYPE minmax GRANULARITY 1;
OPTIMIZE TABLE move_ttl FINAL;
OPTIMIZE TABLE move_ttl_control FINAL;
OPTIMIZE TABLE recompression_ttl FINAL;
OPTIMIZE TABLE recompression_ttl_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'move_ttl%' OR table LIKE 'recompression_ttl%' ORDER BY table;

-- Column TTL, vertical merge, TTL merges stopped: the expired values survive.
CREATE TABLE column_ttl (id UInt64, d DateTime, c1 UInt64, c2 UInt64 TTL d + INTERVAL 1 DAY, c3 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
CREATE TABLE column_ttl_control AS column_ttl;
SYSTEM STOP TTL MERGES column_ttl;
SYSTEM STOP TTL MERGES column_ttl_control;
INSERT INTO column_ttl SELECT number, if(number < 5, '2000-01-01', '2100-01-01'), 1, 7, 1 FROM numbers(10);
INSERT INTO column_ttl SELECT number + 10, '2100-01-01', 2, 7, 2 FROM numbers(10);
INSERT INTO column_ttl_control SELECT * FROM column_ttl WHERE id < 10;
INSERT INTO column_ttl_control SELECT * FROM column_ttl WHERE id >= 10;
ALTER TABLE column_ttl ADD COLUMN x UInt64;
ALTER TABLE column_ttl ADD INDEX idx (x, id) TYPE minmax GRANULARITY 1;
OPTIMIZE TABLE column_ttl FINAL;
OPTIMIZE TABLE column_ttl_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'column_ttl%' ORDER BY table;
SELECT 'column_ttl', countIf(c2 = 7), countIf(c2 = 0) FROM column_ttl;
SELECT 'column_ttl_control', countIf(c2 = 7), countIf(c2 = 0) FROM column_ttl_control;
SYSTEM START TTL MERGES column_ttl;
SYSTEM START TTL MERGES column_ttl_control;
OPTIMIZE TABLE column_ttl FINAL;
OPTIMIZE TABLE column_ttl_control FINAL;
SELECT table, rows FROM merged_parts WHERE table LIKE 'column_ttl%' ORDER BY table;
SELECT 'column_ttl', countIf(c2 = 7), countIf(c2 = 0) FROM column_ttl;
SELECT 'column_ttl_control', countIf(c2 = 7), countIf(c2 = 0) FROM column_ttl_control;

-- GROUP BY TTL, not due, vertical merge.
CREATE TABLE group_by_ttl (id UInt64, d DateTime, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY (id, d) TTL d + INTERVAL 1 DAY GROUP BY id SET c1 = sum(c1)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
CREATE TABLE group_by_ttl_control AS group_by_ttl;
INSERT INTO group_by_ttl SELECT number % 5, toDateTime('2100-01-01') + number, 1, 1, 1 FROM numbers(10);
INSERT INTO group_by_ttl SELECT number % 5, toDateTime('2100-01-01') + number, 2, 2, 2 FROM numbers(10);
INSERT INTO group_by_ttl_control SELECT * FROM group_by_ttl WHERE c1 = 1;
INSERT INTO group_by_ttl_control SELECT * FROM group_by_ttl WHERE c1 = 2;
ALTER TABLE group_by_ttl ADD COLUMN x UInt64;
ALTER TABLE group_by_ttl ADD INDEX idx (x, id) TYPE minmax GRANULARITY 1;
OPTIMIZE TABLE group_by_ttl FINAL;
OPTIMIZE TABLE group_by_ttl_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'group_by_ttl%' ORDER BY table;
