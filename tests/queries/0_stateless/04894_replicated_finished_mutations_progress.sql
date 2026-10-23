-- Tags: zookeeper
-- A replicated table whose mutations are all finished reports them as done without sizing its parts.

DROP TABLE IF EXISTS t_finished_mutations SYNC;
CREATE TABLE t_finished_mutations (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_finished_mutations', 'r1') ORDER BY k;

INSERT INTO t_finished_mutations SELECT number, number FROM numbers(1000);
INSERT INTO t_finished_mutations SELECT number, number FROM numbers(1000);

ALTER TABLE t_finished_mutations UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_finished_mutations DELETE WHERE k < 10 SETTINGS mutations_sync = 2;

SELECT is_done, parts_to_do, bytes_to_do, progress FROM system.mutations
WHERE database = currentDatabase() AND table = 't_finished_mutations' ORDER BY mutation_id;

DROP TABLE t_finished_mutations SYNC;
