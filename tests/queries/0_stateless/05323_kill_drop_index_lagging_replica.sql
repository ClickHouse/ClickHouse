-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: the test needs two replicas of its own with
-- different mutation progress.

-- `KILL MUTATION` takes a mutation away from every replica, so a replica that has applied a
-- `DROP INDEX` must not forget it on behalf of one that has not: the parts of the lagging replica
-- still hold the dropped index's granules, and the metadata has given the name to a new index.

DROP TABLE IF EXISTS t_ixkr1 SYNC;
DROP TABLE IF EXISTS t_ixkr2 SYNC;

CREATE TABLE t_ixkr1 (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323_kill_drop_index', 'r1')
ORDER BY tuple() SETTINGS index_granularity = 128;

CREATE TABLE t_ixkr2 (a UInt32, b UInt32, INDEX ix a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323_kill_drop_index', 'r2')
ORDER BY tuple() SETTINGS index_granularity = 128;

INSERT INTO t_ixkr1 SELECT number + 1000000, 999999 - number FROM numbers(10000);
SYSTEM SYNC REPLICA t_ixkr2;

-- `r2` executes nothing from here on, so it keeps the files of the dropped index while `r1`
-- rewrites its own part.
SYSTEM STOP REPLICATION QUEUES t_ixkr2;

ALTER TABLE t_ixkr1 DROP INDEX ix, ADD INDEX ix b TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 1;

SELECT '-- the drop is done on the replica the kill is issued on';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ixkr1' AND NOT is_done;

SELECT '-- the kill is refused anyway, because the other replica has not applied it';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_ixkr1'; -- { serverError BAD_ARGUMENTS }

SELECT '-- and the other replica still answers without the stale granules once it catches up';
SYSTEM START REPLICATION QUEUES t_ixkr2;
SYSTEM SYNC REPLICA t_ixkr2;
SELECT count() FROM t_ixkr2 WHERE b = 995000;

DROP TABLE t_ixkr1 SYNC;
DROP TABLE t_ixkr2 SYNC;
