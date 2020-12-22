DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;

CREATE TABLE r1 (
    key UInt64, value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/01509_no_repliacs', '1')
ORDER BY tuple();

CREATE TABLE r2 (
    key UInt64, value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/01509_no_repliacs', '2')
ORDER BY tuple();

SET insert_quorum_parallel=1;

SET insert_quorum=3;
INSERT INTO r1 VALUES(1, '1'); --{serverError 285}

SELECT 'insert to two replicas works';
SET insert_quorum=2, insert_quorum_parallel=1;
INSERT INTO r1 VALUES(1, '1');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DETACH TABLE r2;

INSERT INTO r1 VALUES(2, '2'); --{serverError 285}

SET insert_quorum=1, insert_quorum_parallel=1;
SELECT 'insert to single replica works';
INSERT INTO r1 VALUES(2, '2');

ATTACH TABLE r2;

SYSTEM SYNC REPLICA r2;

SET insert_quorum=2, insert_quorum_parallel=1;

INSERT INTO r1 VALUES(3, '3');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

SELECT 'deduplication works';
INSERT INTO r2 VALUES(3, '3');

SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

SYSTEM STOP FETCHES r2;

SET insert_quorum_timeout=0;

INSERT INTO r1 VALUES (4, '4'); -- { serverError 319 }

SYSTEM START FETCHES r2;

SYSTEM SYNC REPLICA r2;

SELECT 'insert happened';
SELECT COUNT() FROM r1;
SELECT COUNT() FROM r2;

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;
