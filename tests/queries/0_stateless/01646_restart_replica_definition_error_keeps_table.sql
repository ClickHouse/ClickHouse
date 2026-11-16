-- Tags: zookeeper

-- A `SYSTEM RESTART REPLICA` that fails because the table definition cannot be analyzed
-- must leave the table in place instead of making it disappear until server restart.

DROP TABLE IF EXISTS t_check SYNC;
DROP TABLE IF EXISTS t_probe SYNC;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_check (x UInt64, CONSTRAINT c CHECK x < (SELECT count() + 1000 FROM t_probe))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_check', 'r1') ORDER BY tuple();

-- The constraint now references a missing table.
DROP TABLE t_probe SYNC SETTINGS check_table_dependencies = 0;

SYSTEM RESTART REPLICA t_check; -- { serverError UNKNOWN_TABLE }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
SELECT count() FROM t_check;

-- Once the referenced table is back, the restart succeeds.
CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM RESTART REPLICA t_check;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
SELECT count() FROM t_check;

DROP TABLE t_check SYNC;
DROP TABLE t_probe SYNC;
