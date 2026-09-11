-- Tags: no-replicated-database
-- no-replicated-database: read_rows in query_log differs because of replicated database.

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DROP TABLE IF EXISTS t_lwu_rr_wide SYNC;
DROP TABLE IF EXISTS t_lwu_rr_compact SYNC;
DROP TABLE IF EXISTS t_lwu_rr_v2 SYNC;
DROP TABLE IF EXISTS t_lwu_rr_nonadaptive SYNC;

-- Once a patch part exists, every read of the table starts with a lightweight-delete step that
-- requests only _row_exists, which has no file in the part. With patch_parts_version = 'v1' that
-- step is completed with virtual columns only, so on a Wide part it materializes nothing from disk.
-- add_minmax_index_for_numeric_columns = 0 keeps the expected read_rows exact.
CREATE TABLE t_lwu_rr_wide (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
    add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0, patch_parts_version = 'v1';

-- Control: same patch mode over a Compact part, whose reader returns the granularity-derived count.
CREATE TABLE t_lwu_rr_compact (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
    add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 1000000000, patch_parts_version = 'v1';

-- Control: Wide part again, but patch_parts_version = 'v2' adds the physical sorting key to the step.
CREATE TABLE t_lwu_rr_v2 (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
    add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0, patch_parts_version = 'v2';

-- The same Wide/v1 combination with non-adaptive marks (index_granularity_bytes = 0, which only a
-- Wide part can have) and a partial final granule: 1000 rows at granularity 64 leave a 40-row tail.
CREATE TABLE t_lwu_rr_nonadaptive (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_block_number_column = 1,
    enable_block_offset_column = 1, add_minmax_index_for_numeric_columns = 0,
    patch_parts_version = 'v1';

INSERT INTO t_lwu_rr_wide SELECT * FROM numbers(1000);
INSERT INTO t_lwu_rr_compact SELECT * FROM numbers(1000);
INSERT INTO t_lwu_rr_v2 SELECT * FROM numbers(1000);
INSERT INTO t_lwu_rr_nonadaptive SELECT * FROM numbers(1000);

DELETE FROM t_lwu_rr_wide WHERE id = 200;
DELETE FROM t_lwu_rr_wide WHERE id IN (100, 110, 120, 130);

DELETE FROM t_lwu_rr_compact WHERE id = 200;
DELETE FROM t_lwu_rr_compact WHERE id IN (100, 110, 120, 130);

DELETE FROM t_lwu_rr_v2 WHERE id = 200;
DELETE FROM t_lwu_rr_v2 WHERE id IN (100, 110, 120, 130);

DELETE FROM t_lwu_rr_nonadaptive WHERE id = 200;
DELETE FROM t_lwu_rr_nonadaptive WHERE id IN (100, 110, 120, 130);

SELECT 'part types', table, arraySort(arrayDistinct(groupArray(part_type))) FROM system.parts
WHERE database = currentDatabase() AND table LIKE 't\_lwu\_rr\_%' AND active
GROUP BY table ORDER BY table;

SELECT 'rows left', 't_lwu_rr_wide', count(), countIf(id IN (100, 110, 120, 130, 200)) FROM t_lwu_rr_wide;
SELECT 'rows left', 't_lwu_rr_compact', count(), countIf(id IN (100, 110, 120, 130, 200)) FROM t_lwu_rr_compact;
SELECT 'rows left', 't_lwu_rr_v2', count(), countIf(id IN (100, 110, 120, 130, 200)) FROM t_lwu_rr_v2;
SELECT 'rows left', 't_lwu_rr_nonadaptive', count(), countIf(id IN (100, 110, 120, 130, 200)) FROM t_lwu_rr_nonadaptive;

-- A plain scan of the patched table is accounted through the same step, so it must report the rows
-- of the part it read, not zero.
SELECT 'scan sum', 't_lwu_rr_wide', sum(id) FROM t_lwu_rr_wide SETTINGS log_comment = 'lwu_rr_scan_1_wide';
SELECT 'scan sum', 't_lwu_rr_compact', sum(id) FROM t_lwu_rr_compact SETTINGS log_comment = 'lwu_rr_scan_2_compact';
SELECT 'scan sum', 't_lwu_rr_v2', sum(id) FROM t_lwu_rr_v2 SETTINGS log_comment = 'lwu_rr_scan_3_v2';
SELECT 'scan sum', 't_lwu_rr_nonadaptive', sum(id) FROM t_lwu_rr_nonadaptive SETTINGS log_comment = 'lwu_rr_scan_4_nonadaptive';

SYSTEM FLUSH LOGS query_log;

SELECT 'delete read_rows', extract(query, 'DELETE FROM (\\w+)') AS name, read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND is_initial_query
    AND current_database = currentDatabase() AND query LIKE 'DELETE FROM t\_lwu\_rr\_%'
ORDER BY name, event_time_microseconds;

-- The four scans read the same rows, so they must report the same non-zero count. This is asserted
-- against the v2 arm, which reads the physical sorting key, rather than against a literal: the DELETE
-- counts above are flavour-stable and pinned exactly, a full-scan count has no such precedent.
SELECT 'scan read_rows', count() AS scans, uniqExact(read_rows) AS distinct_counts,
    min(read_rows) > 0 AS all_non_zero
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND is_initial_query
    AND current_database = currentDatabase() AND log_comment LIKE 'lwu\_rr\_scan\_%';

-- The tail arm is pinned to the row count of the INSERT instead: a non-adaptive part's last mark is
-- padded to a full granule until the part's row count corrects it, which would report 1024 here.
SELECT 'nonadaptive scan read_rows', read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND is_initial_query
    AND current_database = currentDatabase() AND log_comment = 'lwu_rr_scan_4_nonadaptive';

DROP TABLE t_lwu_rr_wide SYNC;
DROP TABLE t_lwu_rr_compact SYNC;
DROP TABLE t_lwu_rr_v2 SYNC;
DROP TABLE t_lwu_rr_nonadaptive SYNC;
