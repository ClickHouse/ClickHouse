SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS attach_r1;
DROP TABLE IF EXISTS attach_r2;

CREATE TABLE attach_r1 (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01452/attach', 'r1') order by tuple() settings max_replicated_merges_in_queue = 0;
CREATE TABLE attach_r2 (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01452/attach', 'r2') order by tuple() settings max_replicated_merges_in_queue = 0;

INSERT INTO attach_r1 VALUES (0);
INSERT INTO attach_r1 VALUES (1);
INSERT INTO attach_r1 VALUES (2);

SELECT v FROM attach_r1 ORDER BY v;

ALTER TABLE attach_r2 DROP PART 'all_1_1_0';

SELECT v FROM attach_r1 ORDER BY v;

ALTER TABLE attach_r1 MODIFY SETTING max_replicated_merges_in_queue = 1;
OPTIMIZE TABLE attach_r1 FINAL;

SELECT name FROM system.parts WHERE table = 'attach_r1' AND active;

DROP TABLE attach_r1;
DROP TABLE attach_r2;
