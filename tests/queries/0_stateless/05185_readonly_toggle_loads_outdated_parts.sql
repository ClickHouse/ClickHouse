CREATE TABLE readonly_outdated (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
SYSTEM STOP CLEANUP readonly_outdated;
INSERT INTO readonly_outdated SELECT number FROM numbers(10);
INSERT INTO readonly_outdated SELECT number + 10 FROM numbers(10);
OPTIMIZE TABLE readonly_outdated FINAL;

-- `TRUNCATE` may leave the merged part for a later cleanup pass; the two inserted parts remain outdated on disk.
TRUNCATE TABLE readonly_outdated;
ALTER TABLE readonly_outdated MODIFY SETTING table_readonly = 1;
CREATE TABLE outdated_before_detach (name String) ENGINE = Memory;
INSERT INTO outdated_before_detach SELECT name FROM system.parts
WHERE database = currentDatabase() AND table = 'readonly_outdated' AND NOT active AND rows > 0;
DETACH TABLE readonly_outdated;
ATTACH TABLE readonly_outdated;

-- Read-only operations must not wait for the deferred loading task.
SYSTEM WAIT LOADING PARTS readonly_outdated;
SYSTEM STOP CLEANUP readonly_outdated;
ALTER TABLE readonly_outdated MODIFY SETTING table_readonly = 0;
SYSTEM WAIT LOADING PARTS readonly_outdated;

-- Cleanup is stopped, so the empty cover, both inserted parts and every outdated part from before `DETACH` must exist.
SELECT countIf(active AND rows = 0) = 1, countIf(NOT active AND level = 0) = 2,
    arraySort(groupArrayIf(name, NOT active AND rows > 0)) = (SELECT arraySort(groupArray(name)) FROM outdated_before_detach)
FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_outdated';
SELECT count() FROM readonly_outdated;

SYSTEM START CLEANUP readonly_outdated;
DETACH TABLE readonly_outdated;
ATTACH TABLE readonly_outdated;
SELECT count() FROM readonly_outdated;
DROP TABLE readonly_outdated SYNC;
DROP TABLE outdated_before_detach;
