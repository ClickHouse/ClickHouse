-- Tags: zookeeper

-- A `SYSTEM RESTART REPLICA` of a table whose definition refers to a missing table
-- must leave the table in place instead of making it disappear until server restart.

DROP TABLE IF EXISTS t_check SYNC;
DROP TABLE IF EXISTS t_probe SYNC;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_check (x UInt64, CONSTRAINT c CHECK x NOT IN (SELECT k + 1000 FROM t_probe))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_check', 'r1') ORDER BY tuple();

-- The constraint now references a missing table.
DROP TABLE t_probe SYNC SETTINGS check_table_dependencies = 0;

-- The subquery of a `CHECK` constraint is not analyzed when the table is loaded: its set is built
-- at insert time. So the restart succeeds, and it is the insert that reports the missing table.
SYSTEM RESTART REPLICA t_check;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
INSERT INTO t_check SETTINGS async_insert = 0 VALUES (1); -- { serverError UNKNOWN_TABLE }
SELECT count() FROM t_check;

-- Once the referenced table is back, the constraint works again.
CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM RESTART REPLICA t_check;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
INSERT INTO t_check SETTINGS async_insert = 0 VALUES (1);
SELECT count() FROM t_check;

DROP TABLE t_check SYNC;
DROP TABLE t_probe SYNC;
