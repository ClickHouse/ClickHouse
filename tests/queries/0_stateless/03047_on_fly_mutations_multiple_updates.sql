-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel
-- no-parallel: SYSTEM DROP MARK CACHE is used.

DROP TABLE IF EXISTS t_lightweight_mut_5;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_lightweight_mut_5 (id UInt64, s1 String, s2 String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    serialization_info_version = 'basic',
    storage_policy = 'default';

SYSTEM STOP MERGES t_lightweight_mut_5;

INSERT INTO t_lightweight_mut_5 VALUES (1, 'a', 'b');
ALTER TABLE t_lightweight_mut_5 UPDATE s1 = 'x', s2 = 'y' WHERE id = 1;

SYSTEM DROP MARK CACHE;
SELECT s1 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT s2 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT s1, s2 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM FLUSH LOGS query_log;

SELECT query, ProfileEvents['FileOpen'] FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query ILIKE 'SELECT%FROM t_lightweight_mut_5%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_lightweight_mut_5;
