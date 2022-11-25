-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

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
INSERT INTO r1 VALUES(1, '1'); --{serverError 285}

-- retry should still fail despite the insert_deduplicate enabled
INSERT INTO r1 VALUES(1, '1'); --{serverError 285}
INSERT INTO r1 VALUES(1, '1'); --{serverError 285}

SELECT 'insert to two replicas works';
SET insert_quorum=2, insert_quorum_parallel=1;

INSERT INTO r1 VALUES(1, '1');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DETACH TABLE r2;

INSERT INTO r1 VALUES(2, '2'); --{serverError 285}

-- retry should fail despite the insert_deduplicate enabled
INSERT INTO r1 VALUES(2, '2'); --{serverError 285}
INSERT INTO r1 VALUES(2, '2'); --{serverError 285}

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
INSERT INTO r1 VALUES(3, '3'); --{serverError 285}
-- work back ok when quorum=2
SET insert_quorum=2, insert_quorum_parallel=1;
INSERT INTO r2 VALUES(3, '3');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

SYSTEM STOP FETCHES r2;

SET insert_quorum_timeout=0;

INSERT INTO r1 VALUES (4, '4'); -- { serverError 319 }

-- retry should fail despite the insert_deduplicate enabled
INSERT INTO r1 VALUES (4, '4'); -- { serverError 319 }
INSERT INTO r1 VALUES (4, '4'); -- { serverError 319 }
SELECT * FROM r2 WHERE key=4;

SYSTEM START FETCHES r2;

SET insert_quorum_timeout=6000000;

-- now retry should be successful
INSERT INTO r1 VALUES (4, '4');

SYSTEM SYNC REPLICA r2;

SELECT 'insert happened';
SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;
