-- Tags: zookeeper

DROP TABLE IF EXISTS t_modify_projection_r1;
DROP TABLE IF EXISTS t_modify_projection_r2;

CREATE TABLE t_modify_projection_r1
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 1024)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_modify_projection', 'r1') ORDER BY k;

CREATE TABLE t_modify_projection_r2
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 1024)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_modify_projection', 'r2') ORDER BY k;

-- `alter_sync = 2` waits for every replica to apply the new metadata version. `SYSTEM SYNC REPLICA`
-- alone is not enough: on `SharedMergeTree` it refreshes parts and mutations, not the table structure.
ALTER TABLE t_modify_projection_r1 MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 128)
    SETTINGS alter_sync = 2;

SYSTEM SYNC REPLICA t_modify_projection_r2;

SELECT '-- the metadata alter replicated to the second replica';
SHOW CREATE TABLE t_modify_projection_r2;

DROP TABLE t_modify_projection_r1;
DROP TABLE t_modify_projection_r2;
