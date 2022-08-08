-- Tags: long, zookeeper

SET send_logs_level = 'fatal';
SET insert_quorum_parallel = false;
SET select_sequential_consistency=1;

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;
DROP TABLE IF EXISTS quorum3;

CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum', '2') ORDER BY x PARTITION BY y;

-- majority_insert_quorum = n/2 + 1 , so insert will be written to both replica
SET majority_insert_quorum = true;

INSERT INTO quorum1 VALUES (1, '2018-11-15');
INSERT INTO quorum1 VALUES (2, '2018-11-15');
INSERT INTO quorum1 VALUES (3, '2018-12-16');

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

DROP TABLE quorum1;
DROP TABLE quorum2;

-- check majority_insert_quorum valid values
SET majority_insert_quorum = TrUe;
select * from system.settings where name like 'majority_insert_quorum';
SET majority_insert_quorum = FalSE;
select * from system.settings where name like 'majority_insert_quorum';
-- this is also allowed
SET majority_insert_quorum = 10;
select * from system.settings where name like 'majority_insert_quorum';

-- Create 3 replicas and stop sync 2 replicas
CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum1', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum1', '2') ORDER BY x PARTITION BY y;
CREATE TABLE quorum3(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum1', '3') ORDER BY x PARTITION BY y;

SET majority_insert_quorum = true;

-- Insert should be successful
-- stop replica 3
SYSTEM STOP FETCHES quorum3;
INSERT INTO quorum1 VALUES (1, '2018-11-15');
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x; -- {serverError 289}

-- Sync replica 3
SYSTEM START FETCHES quorum3;
SYSTEM SYNC REPLICA quorum3;
SELECT x FROM quorum3 ORDER BY x;

-- Stop 2 replicas , so insert wont be successful
SYSTEM STOP FETCHES quorum2;
SYSTEM STOP FETCHES quorum3;
SET insert_quorum_timeout = 5000;
INSERT INTO quorum1 VALUES (2, '2018-11-15'); -- { serverError 319 }
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x;

-- Sync replica 2 and 3
SYSTEM START FETCHES quorum2;
SYSTEM SYNC REPLICA quorum2;
SYSTEM START FETCHES quorum3;
SYSTEM SYNC REPLICA quorum3;

INSERT INTO quorum1 VALUES (3, '2018-11-15');
SELECT x FROM quorum1 ORDER BY x;
SYSTEM SYNC REPLICA quorum2;
SYSTEM SYNC REPLICA quorum3;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x;

DROP TABLE quorum1;
DROP TABLE quorum2;
DROP TABLE quorum3;

-- both insert_quorum and majority_insert_quorum are on, in that case max of both will be used as insert quorum
-- insert_quorum < n/2 +1
SET majority_insert_quorum = true;
set insert_quorum = 1;
CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum2', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum2', '2') ORDER BY x PARTITION BY y;
CREATE TABLE quorum3(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02377/quorum2', '3') ORDER BY x PARTITION BY y;

-- Insert should be successful
-- stop replica 3
SYSTEM STOP FETCHES quorum3;
INSERT INTO quorum1 VALUES (11, '2018-11-15');
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x; -- {serverError 289}

-- Sync replica 3
SYSTEM START FETCHES quorum3;
SYSTEM SYNC REPLICA quorum3;
SELECT x FROM quorum3 ORDER BY x;

-- insert_quorum > n/2 +1
set insert_quorum = 3;
SET majority_insert_quorum = false;
SYSTEM STOP FETCHES quorum3;
INSERT INTO quorum1 VALUES (12, '2018-11-15'); -- { serverError 319 }
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x;

-- Sync replica 3
SYSTEM START FETCHES quorum3;
SYSTEM SYNC REPLICA quorum3;
INSERT INTO quorum1 VALUES (13, '2018-11-15');
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;
SELECT x FROM quorum3 ORDER BY x;

DROP TABLE quorum1;
DROP TABLE quorum2;
DROP TABLE quorum3;
