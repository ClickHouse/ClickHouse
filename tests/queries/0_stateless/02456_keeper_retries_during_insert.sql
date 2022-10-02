-- Tags: replica

DROP TABLE IF EXISTS keeper_retries_r1 SYNC;
DROP TABLE IF EXISTS keeper_retries_r2 SYNC;

CREATE TABLE keeper_retries_r1(a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/02456_keeper_retries_during_insert', 'r1') ORDER BY tuple ();
CREATE TABLE keeper_retries_r2(a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/02456_keeper_retries_during_insert', 'r2') ORDER BY tuple();

INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=0 VALUES (1);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0, insert_keeper_max_retries=0 VALUES (2);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_max_retries=0 VALUES (3); -- { serverError KEEPER_EXCEPTION }
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_retry_max_backoff_ms=10 VALUES (4); -- { serverError KEEPER_EXCEPTION }

SET insert_quorum=2;
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=0 VALUES (11);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0, insert_keeper_max_retries=0 VALUES (12);
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_max_retries=0 VALUES (13); -- { serverError KEEPER_EXCEPTION }
INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=1, insert_keeper_retry_max_backoff_ms=10 VALUES (14); -- { serverError KEEPER_EXCEPTION }

-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0.05, insert_keeper_fault_injection_seed=0 VALUES (21);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0.2, insert_keeper_max_retries=100, insert_keeper_retry_max_backoff_ms=10, insert_keeper_fault_injection_seed=2 VALUES (22);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0.3, insert_keeper_max_retries=100, insert_keeper_retry_max_backoff_ms=10, insert_keeper_fault_injection_seed=3 VALUES (23);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0.4, insert_keeper_max_retries=100, insert_keeper_retry_max_backoff_ms=10, insert_keeper_fault_injection_seed=4 VALUES (24);
-- INSERT INTO keeper_retries_r1 SETTINGS insert_keeper_fault_injection_mode=1, insert_keeper_fault_injection_probability=0.5, insert_keeper_max_retries=300, insert_keeper_retry_max_backoff_ms=10, insert_keeper_fault_injection_seed=4 VALUES (25);

SELECT * FROM keeper_retries_r1 order by a;

DROP TABLE keeper_retries_r1 SYNC;
DROP TABLE keeper_retries_r2 SYNC;
