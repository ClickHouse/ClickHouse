-- Test: system.detached_parts `bytes_on_disk` column.
-- `StorageSystemDetachedParts` has a dedicated `calculatePartSizeOnDisk` /
-- `calculateTotalSizeOnDisk` code path (src/Storages/System/StorageSystemDetachedParts.cpp
-- lines 34-60 and 161-198) that is only activated when the caller requests the
-- `bytes_on_disk` column (column index 4 in the columns mask, line 211).
-- All existing CI queries for this table use `SELECT * EXCEPT (bytes_on_disk, ...)`,
-- so that code path was never exercised.
-- Without this path, system.detached_parts.bytes_on_disk would always return 0
-- instead of the actual on-disk size of the detached part directory.
-- Also exercises the applyFilters false branch (line 339: filter_actions_dag is null
-- when querying without WHERE) and the non-extractable-filter branch (line 351:
-- filter_actions_dag present but the predicate column is not in the allowed fast-path set).

DROP TABLE IF EXISTS t_detach_bytes;
CREATE TABLE t_detach_bytes (n Int64, p Int32) ENGINE = MergeTree ORDER BY n PARTITION BY p;
INSERT INTO t_detach_bytes SELECT number, number % 3 FROM numbers(300);

-- Detach one partition so system.detached_parts has at least one row.
ALTER TABLE t_detach_bytes DETACH PARTITION 0;

-- Exercise calculatePartSizeOnDisk (lines 161-198) + calculateTotalSizeOnDisk (lines 34-60).
-- The detached part contains real data so bytes_on_disk must be > 0.
SELECT bytes_on_disk > 0 AS has_bytes
FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_detach_bytes'
ORDER BY has_bytes;

-- Exercise applyFilters false branch (line 339): no WHERE clause means
-- filter_actions_dag is null and the predicate extraction block is entirely skipped.
SELECT count() >= 0 AS ok FROM system.detached_parts;

-- Exercise filter=null branch (line 351): WHERE on bytes_on_disk is not in the
-- fast-path allowed-inputs block (database/table/engine/active/uuid), so
-- splitFilterDagForAllowedInputs returns null even though filter_actions_dag is set.
SELECT count() >= 0 AS ok FROM system.detached_parts WHERE bytes_on_disk >= 0;

DROP TABLE t_detach_bytes;
