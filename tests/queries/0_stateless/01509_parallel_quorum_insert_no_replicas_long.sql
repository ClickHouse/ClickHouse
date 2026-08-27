-- Tags: long, replica, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- no-shared-merge-tree: depend on tricks with quorum inserts for replicated mt

DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;

CREATE TABLE r1 (
    key UInt64, value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01509_parallel_quorum_insert_no_replicas', '1')
ORDER BY tuple();

CREATE TABLE r2 (
    key UInt64, value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01509_parallel_quorum_insert_no_replicas', '2')
ORDER BY tuple();

SET insert_quorum_parallel=1;

SET insert_quorum=3;
INSERT INTO r1 VALUES(1, '1'); --{serverError TOO_FEW_LIVE_REPLICAS}

-- retry should still fail despite the insert_deduplicate enabled
INSERT INTO r1 VALUES(1, '1'); --{serverError TOO_FEW_LIVE_REPLICAS}
INSERT INTO r1 VALUES(1, '1'); --{serverError TOO_FEW_LIVE_REPLICAS}

SELECT 'insert to two replicas works';
SET insert_quorum=2, insert_quorum_parallel=1;

INSERT INTO r1 VALUES(1, '1');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DETACH TABLE r2;

INSERT INTO r1 VALUES(2, '2'); --{serverError TOO_FEW_LIVE_REPLICAS}

-- retry should fail despite the insert_deduplicate enabled
INSERT INTO r1 VALUES(2, '2'); --{serverError TOO_FEW_LIVE_REPLICAS}
INSERT INTO r1 VALUES(2, '2'); --{serverError TOO_FEW_LIVE_REPLICAS}

SET insert_quorum=1, insert_quorum_parallel=1;
SELECT 'insert to single replica works';
INSERT INTO r1 VALUES(2, '2');

ATTACH TABLE r2;

INSERT INTO r2 VALUES(2, '2');

SYSTEM SYNC REPLICA r2;

SET insert_quorum=2, insert_quorum_parallel=1;

INSERT INTO r1 VALUES(3, '3');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

SELECT 'deduplication works';
INSERT INTO r2 VALUES(3, '3');

-- still works if we relax quorum
SET insert_quorum=1, insert_quorum_parallel=1;
INSERT INTO r2 VALUES(3, '3');
INSERT INTO r1 VALUES(3, '3');
-- will start failing if we increase quorum
SET insert_quorum=3, insert_quorum_parallel=1;
INSERT INTO r1 VALUES(3, '3'); --{serverError TOO_FEW_LIVE_REPLICAS}
-- work back ok when quorum=2
SET insert_quorum=2, insert_quorum_parallel=1;
INSERT INTO r2 VALUES(3, '3');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

SYSTEM STOP FETCHES r2;

SET insert_quorum_timeout=0;

INSERT INTO r1 SETTINGS insert_keeper_fault_injection_probability=0 VALUES (4, '4'); -- { serverError UNKNOWN_STATUS_OF_INSERT }

-- retry should fail despite the insert_deduplicate enabled
INSERT INTO r1 SETTINGS insert_keeper_fault_injection_probability=0 VALUES (4, '4'); -- { serverError UNKNOWN_STATUS_OF_INSERT }
INSERT INTO r1 SETTINGS insert_keeper_fault_injection_probability=0 VALUES (4, '4'); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SELECT * FROM r2 WHERE key=4;

SYSTEM START FETCHES r2;

SET insert_quorum_timeout=6000000;

-- now retry should be successful
INSERT INTO r1 VALUES (4, '4');

SYSTEM SYNC REPLICA r2;

SELECT 'insert happened';
SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;
