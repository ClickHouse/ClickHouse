SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;

CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '2') ORDER BY x PARTITION BY y;

SET insert_quorum=2;
SET select_sequential_consistency=1;

SET insert_quorum_timeout=0;

SYSTEM STOP FETCHES quorum1;

INSERT INTO quorum2 VALUES (1, '2018-11-15'); -- { serverError 319 }

SELECT count(*) FROM quorum1;
SELECT count(*) FROM quorum2;

SET select_sequential_consistency=0;

SELECT x FROM quorum2 ORDER BY x;
SET select_sequential_consistency=1;

SET insert_quorum_timeout=100;

SYSTEM START FETCHES quorum1;
SYSTEM SYNC REPLICA quorum1;
 
SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

DROP TABLE quorum1 NO DELAY;
DROP TABLE quorum2 NO DELAY;
SELECT sleep(1) FORMAT Null;
