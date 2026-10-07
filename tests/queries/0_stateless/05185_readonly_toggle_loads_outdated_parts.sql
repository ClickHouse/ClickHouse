CREATE TABLE readonly_outdated (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
SYSTEM STOP CLEANUP readonly_outdated;
INSERT INTO readonly_outdated SELECT number FROM numbers(10);
INSERT INTO readonly_outdated SELECT number + 10 FROM numbers(10);
OPTIMIZE TABLE readonly_outdated FINAL;

-- The merged part is removed by `TRUNCATE`; its two original parts remain outdated on disk.
TRUNCATE TABLE readonly_outdated;
ALTER TABLE readonly_outdated MODIFY SETTING table_readonly = 1;
DETACH TABLE readonly_outdated;
ATTACH TABLE readonly_outdated;

-- Read-only operations must not wait for the deferred loading task.
SYSTEM WAIT LOADING PARTS readonly_outdated;
SYSTEM STOP CLEANUP readonly_outdated;
ALTER TABLE readonly_outdated MODIFY SETTING table_readonly = 0;
SYSTEM WAIT LOADING PARTS readonly_outdated;

-- Cleanup is stopped, so both the empty cover and the loaded outdated parts must still exist.
SELECT countIf(active AND rows = 0) = 1, countIf(NOT active AND rows > 0) = 2
FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_outdated';
SELECT count() FROM readonly_outdated;

SYSTEM START CLEANUP readonly_outdated;
DETACH TABLE readonly_outdated;
ATTACH TABLE readonly_outdated;
SELECT count() FROM readonly_outdated;
DROP TABLE readonly_outdated SYNC;
