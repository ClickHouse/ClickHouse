SELECT '*** Replicated with sampling ***';

DROP TABLE IF EXISTS test.replicated_with_sampling;

CREATE TABLE test.replicated_with_sampling(x UInt8)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/replicated_with_sampling', 'r1')
    ORDER BY x
    SAMPLE BY x;

INSERT INTO test.replicated_with_sampling VALUES (1), (128);
SELECT sum(x) FROM test.replicated_with_sampling SAMPLE 1/2;

DROP TABLE test.replicated_with_sampling;

SELECT '*** Replacing with implicit version ***';

DROP TABLE IF EXISTS test.replacing;

CREATE TABLE test.replacing(d Date, x UInt32, s String) ENGINE = ReplacingMergeTree ORDER BY x PARTITION BY d;

INSERT INTO test.replacing VALUES ('2017-10-23', 1, 'a');
INSERT INTO test.replacing VALUES ('2017-10-23', 1, 'b');
INSERT INTO test.replacing VALUES ('2017-10-23', 1, 'c');

OPTIMIZE TABLE test.replacing PARTITION '2017-10-23' FINAL;

SELECT * FROM test.replacing;

DROP TABLE test.replacing;

SELECT '*** Replicated Collapsing ***';

DROP TABLE IF EXISTS test.replicated_collapsing;

CREATE TABLE test.replicated_collapsing(d Date, x UInt32, sign Int8)
    ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test/replicated_collapsing', 'r1', sign)
    PARTITION BY toYYYYMM(d) ORDER BY d;

INSERT INTO test.replicated_collapsing VALUES ('2017-10-23', 1, 1);
INSERT INTO test.replicated_collapsing VALUES ('2017-10-23', 1, -1), ('2017-10-23', 2, 1);

OPTIMIZE TABLE test.replicated_collapsing PARTITION 201710 FINAL;

SELECT * FROM test.replicated_collapsing;

DROP TABLE test.replicated_collapsing;

SELECT '*** Replicated VersionedCollapsing ***';

DROP TABLE IF EXISTS test.replicated_versioned_collapsing;

CREATE TABLE test.replicated_versioned_collapsing(d Date, x UInt32, sign Int8, version UInt8)
    ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test/replicated_versioned_collapsing', 'r1', sign, version)
    PARTITION BY toYYYYMM(d) ORDER BY (d, version);

INSERT INTO test.replicated_versioned_collapsing VALUES ('2017-10-23', 1, 1, 0);
INSERT INTO test.replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 0), ('2017-10-23', 2, 1, 0);
INSERT INTO test.replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 1), ('2017-10-23', 2, 1, 2);

OPTIMIZE TABLE test.replicated_versioned_collapsing PARTITION 201710 FINAL;

SELECT * FROM test.replicated_versioned_collapsing;

DROP TABLE test.replicated_versioned_collapsing;

SELECT '*** Table definition with SETTINGS ***';

DROP TABLE IF EXISTS test.with_settings;

CREATE TABLE test.with_settings(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/with_settings', 'r1')
    ORDER BY x
    SETTINGS replicated_can_become_leader = 0;

SELECT sleep(1); -- If replicated_can_become_leader were true, this replica would become the leader after 1 second.

SELECT is_leader FROM system.replicas WHERE database = 'test' AND table = 'with_settings';

DROP TABLE test.with_settings;
