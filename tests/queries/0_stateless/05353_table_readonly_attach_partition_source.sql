-- Tags: no-replicated-database, no-shared-merge-tree
-- `table_readonly` is a plain MergeTree setting.

-- A table attached read-only keeps its outdated parts unloaded on disk. It can still be the source
-- of `ATTACH PARTITION ... FROM`: the destination gets all the rows, the source stays intact, and
-- turning `table_readonly` off afterwards loads the deferred outdated parts.

DROP TABLE IF EXISTS readonly_src SYNC;
DROP TABLE IF EXISTS readonly_dst SYNC;

CREATE TABLE readonly_src (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;
SYSTEM STOP CLEANUP readonly_src;
INSERT INTO readonly_src SELECT number FROM numbers(10);
INSERT INTO readonly_src SELECT number + 10 FROM numbers(10);
OPTIMIZE TABLE readonly_src FINAL;
ALTER TABLE readonly_src MODIFY SETTING table_readonly = 1;
DETACH TABLE readonly_src;
ATTACH TABLE readonly_src;
SYSTEM STOP CLEANUP readonly_src;

CREATE TABLE readonly_dst (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS old_parts_lifetime = 3600, min_bytes_for_wide_part = 0;

SELECT 'source parts while read-only', countIf(active), countIf(NOT active)
FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_src';

ALTER TABLE readonly_dst ATTACH PARTITION ID 'all' FROM readonly_src;
SELECT 'destination rows', count(), sum(x) FROM readonly_dst;
SELECT 'source rows', count(), sum(x) FROM readonly_src;

-- The source is still read-only.
INSERT INTO readonly_src VALUES (100); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

ALTER TABLE readonly_src MODIFY SETTING table_readonly = 0;
SYSTEM WAIT LOADING PARTS readonly_src;
SELECT 'source parts after the toggle', countIf(active), countIf(NOT active)
FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_src';
INSERT INTO readonly_src VALUES (100);
SELECT 'source rows after the toggle', count() FROM readonly_src;

SYSTEM START CLEANUP readonly_src;
DROP TABLE readonly_src SYNC;
DROP TABLE readonly_dst SYNC;
