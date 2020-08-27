DROP TABLE IF EXISTS replicated_constraints1;
DROP TABLE IF EXISTS replicated_constraints2;

CREATE TABLE replicated_constraints1
(
    a UInt32,
    b UInt32,
    CONSTRAINT a_constraint CHECK a < 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00988/alter_constraints', 'r1') ORDER BY (a);

CREATE TABLE replicated_constraints2
(
    a UInt32,
    b UInt32,
    CONSTRAINT a_constraint CHECK a < 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00988/alter_constraints', 'r2') ORDER BY (a);

INSERT INTO replicated_constraints1 VALUES (1, 2);
INSERT INTO replicated_constraints2 VALUES (3, 4);

SYSTEM SYNC REPLICA replicated_constraints1;
SYSTEM SYNC REPLICA replicated_constraints2;

INSERT INTO replicated_constraints1 VALUES (10, 10); -- { serverError 469 }

ALTER TABLE replicated_constraints1 DROP CONSTRAINT a_constraint;

SYSTEM SYNC REPLICA replicated_constraints2;

INSERT INTO replicated_constraints1 VALUES (10, 10);
INSERT INTO replicated_constraints2 VALUES (10, 10);

ALTER TABLE replicated_constraints1 ADD CONSTRAINT b_constraint CHECK b > 10;

-- Otherwise "Metadata on replica is not up to date with common metadata in Zookeeper. Cannot alter." is possible.
SYSTEM SYNC REPLICA replicated_constraints1;
SYSTEM SYNC REPLICA replicated_constraints2;

ALTER TABLE replicated_constraints2 ADD CONSTRAINT a_constraint CHECK a < 10;

SYSTEM SYNC REPLICA replicated_constraints1;
SYSTEM SYNC REPLICA replicated_constraints2;

INSERT INTO replicated_constraints1 VALUES (10, 11); -- { serverError 469 }
INSERT INTO replicated_constraints2 VALUES (9, 10); -- { serverError 469 }

DROP TABLE replicated_constraints1;
DROP TABLE replicated_constraints2;
