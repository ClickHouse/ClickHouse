SELECT '*** Replicated with sampling ***';

DROP TABLE IF EXISTS replicated_with_sampling;

CREATE TABLE replicated_with_sampling(x UInt8)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00509/replicated_with_sampling', 'r1')
    ORDER BY x
    SAMPLE BY x;

INSERT INTO replicated_with_sampling VALUES (1), (128);
SELECT sum(x) FROM replicated_with_sampling SAMPLE 1/2;

DROP TABLE replicated_with_sampling;

SELECT '*** Replacing with implicit version ***';

DROP TABLE IF EXISTS replacing;

CREATE TABLE replacing(d Date, x UInt32, s String) ENGINE = ReplacingMergeTree ORDER BY x PARTITION BY d;

INSERT INTO replacing VALUES ('2017-10-23', 1, 'a');
INSERT INTO replacing VALUES ('2017-10-23', 1, 'b');
INSERT INTO replacing VALUES ('2017-10-23', 1, 'c');

OPTIMIZE TABLE replacing PARTITION '2017-10-23' FINAL;

SELECT * FROM replacing;

DROP TABLE replacing;

SELECT '*** Replicated Collapsing ***';

DROP TABLE IF EXISTS replicated_collapsing;

CREATE TABLE replicated_collapsing(d Date, x UInt32, sign Int8)
    ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test_00509/replicated_collapsing', 'r1', sign)
    PARTITION BY toYYYYMM(d) ORDER BY d;

INSERT INTO replicated_collapsing VALUES ('2017-10-23', 1, 1);
INSERT INTO replicated_collapsing VALUES ('2017-10-23', 1, -1), ('2017-10-23', 2, 1);

SYSTEM SYNC REPLICA replicated_collapsing;
OPTIMIZE TABLE replicated_collapsing PARTITION 201710 FINAL;

SELECT * FROM replicated_collapsing;

DROP TABLE replicated_collapsing;

SELECT '*** Replicated VersionedCollapsing ***';

DROP TABLE IF EXISTS replicated_versioned_collapsing;

CREATE TABLE replicated_versioned_collapsing(d Date, x UInt32, sign Int8, version UInt8)
    ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test_00509/replicated_versioned_collapsing', 'r1', sign, version)
    PARTITION BY toYYYYMM(d) ORDER BY (d, version);

INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, 1, 0);
INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 0), ('2017-10-23', 2, 1, 0);
INSERT INTO replicated_versioned_collapsing VALUES ('2017-10-23', 1, -1, 1), ('2017-10-23', 2, 1, 2);

SYSTEM SYNC REPLICA replicated_versioned_collapsing;
OPTIMIZE TABLE replicated_versioned_collapsing PARTITION 201710 FINAL;

SELECT * FROM replicated_versioned_collapsing;

DROP TABLE replicated_versioned_collapsing;

SELECT '*** Table definition with SETTINGS ***';

DROP TABLE IF EXISTS with_settings;

CREATE TABLE with_settings(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00509/with_settings', 'r1')
    ORDER BY x
    SETTINGS replicated_can_become_leader = 0;

SELECT sleep(1); -- If replicated_can_become_leader were true, this replica would become the leader after 1 second.

SELECT is_leader FROM system.replicas WHERE database = currentDatabase() AND table = 'with_settings';

DROP TABLE with_settings;
