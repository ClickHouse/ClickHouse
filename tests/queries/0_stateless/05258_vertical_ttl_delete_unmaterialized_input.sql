-- A rows TTL whose input column no source part stores keeps the merge horizontal (#121960).
-- `*_control` tables store that column, so their merge stays a vertical TTL delete.

SET optimize_throw_if_noop = 1;
SET materialize_ttl_after_modify = 0;
SYSTEM FLUSH LOGS part_log;

-- One row per active part: a successful OPTIMIZE leaves one row, with the algorithm of the merge that wrote it.
CREATE VIEW merged_parts AS
SELECT p.table AS table, p.rows AS rows, p.delete_ttl_info_min > toDateTime(0) AS ttl_calculated, l.merge_algorithm AS merge_algorithm
FROM system.parts AS p
LEFT JOIN (SELECT table, part_name, merge_algorithm FROM system.part_log WHERE database = currentDatabase() AND event_type = 'MergeParts') AS l
    ON p.table = l.table AND p.name = l.part_name
WHERE p.database = currentDatabase() AND p.active;

-- The TTL is set with `materialize_ttl_after_modify = 0`, so the parts' TTL stays uncalculated.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps the background pool off regular merges, so the `OPTIMIZE` does the merge.
CREATE TABLE modify_route (id UInt64, v UInt64, w UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_optimize_ttl_delete = 1;
CREATE TABLE modify_route_control (id UInt64, v UInt64, w UInt64, d DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_optimize_ttl_delete = 1;
INSERT INTO modify_route SELECT number, number, number FROM numbers(10);
INSERT INTO modify_route SELECT number + 10, number, number FROM numbers(10);
INSERT INTO modify_route_control SELECT *, 0 FROM modify_route WHERE id < 10;
INSERT INTO modify_route_control SELECT *, 0 FROM modify_route WHERE id >= 10;
ALTER TABLE modify_route ADD COLUMN d DateTime;
ALTER TABLE modify_route MODIFY TTL d + INTERVAL 100 YEAR;
ALTER TABLE modify_route_control MODIFY TTL d + INTERVAL 100 YEAR;
OPTIMIZE TABLE modify_route FINAL;
OPTIMIZE TABLE modify_route_control FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT * FROM merged_parts WHERE table LIKE 'modify_route%' ORDER BY table;
