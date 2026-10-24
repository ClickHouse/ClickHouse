-- The stress runner passes ignore_drop_queries_probability=0.2 to every client, which turns a DROP
-- of a table with data on disk into a silent no-op; every DROP below is load-bearing.
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS t_05137;
DROP TABLE IF EXISTS dl_05137;

CREATE TABLE t_05137 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05137 VALUES (1);
CREATE TABLE dl_05137 AS loop(currentDatabase(), t_05137);

-- The first read materialises the persisted proxy's nested storage, which is what arms the bug.
SELECT * FROM dl_05137 LIMIT 1;

-- The persisted table must not keep the source's storage object alive.
-- max_execution_time bounds the wait so a regression fails instead of hanging the job.
DROP TABLE t_05137 SYNC SETTINGS max_execution_time = 30;

-- The source left the drop queue, so its data can be reclaimed. This does not depend on the timeout.
SELECT count() FROM system.dropped_tables WHERE database = currentDatabase() AND table = 't_05137';

SELECT * FROM dl_05137 LIMIT 1; -- { serverError UNKNOWN_TABLE }

-- The source is followed by name, so recreating it makes the loop table readable again.
CREATE TABLE t_05137 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05137 VALUES (2);
SELECT * FROM dl_05137 LIMIT 1;

-- A source recreated with a different type must be converted to the type the table advertises,
-- rather than reaching the client under the cached one.
DROP TABLE t_05137 SYNC SETTINGS max_execution_time = 30;
CREATE TABLE t_05137 (x String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05137 VALUES ('3');
SELECT * FROM dl_05137 LIMIT 1;

DROP TABLE dl_05137;
DROP TABLE t_05137;
