DROP TABLE IF EXISTS mutate_and_zero_copy_replication1;
DROP TABLE IF EXISTS mutate_and_zero_copy_replication2;

CREATE TABLE mutate_and_zero_copy_replication1
(
    a UInt64,
    b String,
    c Float64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02427_mutate_and_zero_copy_replication/alter', '1')
ORDER BY tuple()
SETTINGS old_parts_lifetime=0, cleanup_delay_period=300, max_cleanup_delay_period=300, cleanup_delay_period_random_add=300, min_bytes_for_wide_part = 0;

CREATE TABLE mutate_and_zero_copy_replication2
(
    a UInt64,
    b String,
    c Float64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02427_mutate_and_zero_copy_replication/alter', '2')
ORDER BY tuple()
SETTINGS old_parts_lifetime=0, cleanup_delay_period=300,  max_cleanup_delay_period=300, cleanup_delay_period_random_add=300;


INSERT INTO mutate_and_zero_copy_replication1 VALUES (1, '1', 1.0);
SYSTEM SYNC REPLICA mutate_and_zero_copy_replication2;

SET mutations_sync=2;

ALTER TABLE mutate_and_zero_copy_replication1 UPDATE a = 2 WHERE 1;

DROP TABLE mutate_and_zero_copy_replication1 SYNC;

DETACH TABLE mutate_and_zero_copy_replication2;
ATTACH TABLE mutate_and_zero_copy_replication2;

SELECT * FROM mutate_and_zero_copy_replication2 WHERE NOT ignore(*);

DROP TABLE IF EXISTS mutate_and_zero_copy_replication1;
DROP TABLE IF EXISTS mutate_and_zero_copy_replication2;
