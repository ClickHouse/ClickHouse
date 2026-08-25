-- Tags: no-replicated-database
-- no-replicated-database: test relies on system.part_log but mutation can be executed on the second replica

DROP TABLE IF EXISTS t_delete_empty_part_rmt;

CREATE TABLE t_delete_empty_part_rmt (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete_empty_part_rmt', '1')
ORDER BY b PARTITION BY a;

SET insert_keeper_fault_injection_probability = 0.0;

INSERT INTO t_delete_empty_part_rmt SELECT 1, number FROM numbers(1000);
INSERT INTO t_delete_empty_part_rmt SELECT 2, number FROM numbers(1000);
INSERT INTO t_delete_empty_part_rmt SELECT 3, number FROM numbers(2000, 1000);

SET mutations_sync = 2;
ALTER TABLE t_delete_empty_part_rmt DELETE WHERE a = 2 OR b < 500;

SELECT count() FROM t_delete_empty_part_rmt;

SYSTEM FLUSH LOGS part_log;

SELECT
    part_name,
    ProfileEvents['MutationTotalParts'],
    ProfileEvents['MutationUntouchedParts'],
    ProfileEvents['MutationCreatedEmptyParts']
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_delete_empty_part_rmt' AND event_type = 'MutatePart'
ORDER BY part_name;

DROP TABLE t_delete_empty_part_rmt;
