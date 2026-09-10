-- Mutation partition pruning ruled a partition out of a mutation because the parts the initiating
-- replica holds in it are all empty - the state a delete-all mutation or a `TTL` expiry leaves behind
-- until the cleanup thread removes the part - and recorded it as analyzed, which also kept the scope
-- from being widened with the partitions only ZooKeeper knows. Rows another replica acknowledged in
-- that partition then survived the mutation on every replica.

DROP TABLE IF EXISTS t_pruning_empty_r1 SYNC;
DROP TABLE IF EXISTS t_pruning_empty_r2 SYNC;

CREATE TABLE t_pruning_empty_r1 (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pruning_empty', 'r1')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;

CREATE TABLE t_pruning_empty_r2 (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pruning_empty', 'r2')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;

INSERT INTO t_pruning_empty_r1 SELECT 1, number FROM numbers(50);
INSERT INTO t_pruning_empty_r1 SELECT 2, number FROM numbers(50);
SYSTEM SYNC REPLICA t_pruning_empty_r2;

-- Empty out partition 2 everywhere; the 0-row part stays active.
ALTER TABLE t_pruning_empty_r1 DELETE WHERE p = 2 SETTINGS mutations_sync = 2;
SELECT 'the empty part is still active', count(), sum(rows)
FROM system.parts WHERE database = currentDatabase() AND table = 't_pruning_empty_r1' AND partition_id = '2' AND active;

-- Stands in for ordinary replication lag on the initiator.
SYSTEM STOP REPLICATION QUEUES t_pruning_empty_r1;
INSERT INTO t_pruning_empty_r2 SELECT 2, 1000000 + number FROM numbers(30);

ALTER TABLE t_pruning_empty_r1 DELETE WHERE p = 2 SETTINGS alter_sync = 0;

SELECT 'the mutation is scoped to the partition', arraySort(`block_numbers.partition_id`)
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pruning_empty_r1' AND NOT is_done
ORDER BY mutation_id DESC LIMIT 1;

SYSTEM START REPLICATION QUEUES t_pruning_empty_r1;
SYSTEM SYNC REPLICA t_pruning_empty_r1;
ALTER TABLE t_pruning_empty_r1 DELETE WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'r1', count() FROM t_pruning_empty_r1 WHERE p = 2;
SELECT 'r2', count() FROM t_pruning_empty_r2 WHERE p = 2;
SELECT 'the other partition is untouched', count() FROM t_pruning_empty_r1 WHERE p = 1;

SELECT 'and a partition the predicate does not match stays out of the scope';
DROP TABLE IF EXISTS t_pruning_scope SYNC;
CREATE TABLE t_pruning_scope (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pruning_scope', 'r1')
PARTITION BY p ORDER BY x SETTINGS remove_empty_parts = 0;
INSERT INTO t_pruning_scope SELECT 1, number FROM numbers(10);
INSERT INTO t_pruning_scope SELECT 2, number FROM numbers(10);
ALTER TABLE t_pruning_scope DELETE WHERE p = 2 SETTINGS mutations_sync = 2;
ALTER TABLE t_pruning_scope DELETE WHERE p = 1 SETTINGS alter_sync = 0;
SELECT arraySort(`block_numbers.partition_id`) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pruning_scope' AND NOT is_done
ORDER BY mutation_id DESC LIMIT 1;
ALTER TABLE t_pruning_scope DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_pruning_scope;

DROP TABLE t_pruning_scope SYNC;
DROP TABLE t_pruning_empty_r2 SYNC;
DROP TABLE t_pruning_empty_r1 SYNC;
