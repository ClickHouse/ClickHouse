-- Test: system.detached_parts `bytes_on_disk` column.
-- `StorageSystemDetachedParts` has a dedicated `calculatePartSizeOnDisk` /
-- `calculateTotalSizeOnDisk` code path (src/Storages/System/StorageSystemDetachedParts.cpp
-- lines 34-60 and 161-198) that is only activated when the caller requests the
-- `bytes_on_disk` column (column index 4 in the columns mask, line 211).
-- All existing CI queries for this table use `SELECT * EXCEPT (bytes_on_disk, ...)`,
-- so that code path was never exercised.
-- Without this path, system.detached_parts.bytes_on_disk would always return 0
-- instead of the actual on-disk size of the detached part directory.
-- The query is restricted to this test's table: an unrestricted `system.detached_parts`
-- query would enumerate the tables of every concurrent test on the shared server and,
-- with `bytes_on_disk` requested, stat every file of every detached part it finds.

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

DROP TABLE t_detach_bytes;
