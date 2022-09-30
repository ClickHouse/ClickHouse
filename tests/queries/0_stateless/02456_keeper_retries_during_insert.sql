-- Tags: replica

set database_atomic_wait_for_drop_and_detach_synchronously=true;

DROP TABLE IF EXISTS keeper_retries_r1;
DROP TABLE IF EXISTS keeper_retries_r2;
CREATE TABLE keeper_retries_r1(a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/02456_keeper_retries_during_insert', 'r1') ORDER BY tuple ();
CREATE TABLE keeper_retries_r2(a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/02456_keeper_retries_during_insert', 'r2') ORDER BY tuple();

INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=0 VALUES (1);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0, insert_keeper_max_retries=0 VALUES (2);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_max_retries=0 VALUES (3); -- { serverError KEEPER_EXCEPTION }

SET insert_quorum=2;
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=0 VALUES (11);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0, insert_keeper_max_retries=0 VALUES (12);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_max_retries=0 VALUES (13); -- { serverError KEEPER_EXCEPTION }

-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_probability=0.01, insert_keeper_fault_injection_mode=1 VALUES (21);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_probability=0.01, insert_keeper_fault_injection_mode=1 VALUES (22);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_probability=0.01, insert_keeper_fault_injection_mode=1 VALUES (23);

-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=2 VALUES (31);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=2 VALUES (32);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=2 VALUES (33);

SELECT * FROM keeper_retries_r1 order by a;
