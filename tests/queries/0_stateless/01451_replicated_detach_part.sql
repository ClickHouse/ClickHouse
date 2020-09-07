SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS attach_01451_r1;
DROP TABLE IF EXISTS attach_01451_r2;

CREATE TABLE attach_01451_r1 (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01451/attach', 'r1') order by tuple() settings max_replicated_merges_in_queue = 0;
CREATE TABLE attach_01451_r2 (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01451/attach', 'r2') order by tuple() settings max_replicated_merges_in_queue = 0;

INSERT INTO attach_01451_r1 VALUES (0);
INSERT INTO attach_01451_r1 VALUES (1);
INSERT INTO attach_01451_r1 VALUES (2);

SELECT v FROM attach_01451_r1 ORDER BY v;

SYSTEM SYNC REPLICA attach_01451_r2;
ALTER TABLE attach_01451_r2 DETACH PART 'all_1_1_0';

SELECT v FROM attach_01451_r1 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'attach_01451_r2';

ALTER TABLE attach_01451_r2 ATTACH PART 'all_1_1_0';

SYSTEM SYNC REPLICA attach_01451_r1;
SELECT v FROM attach_01451_r1 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'attach_01451_r2';

DROP TABLE attach_01451_r1;
DROP TABLE attach_01451_r2;
