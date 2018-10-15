SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;

CREATE TABLE test.quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE test.quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '2') ORDER BY x PARTITION BY y;

SET insert_quorum=2;
SET select_sequential_consistency=1;

INSERT INTO test.quorum2 VALUES (1, '15-10-2018');
INSERT INTO test.quorum2 VALUES (2, '15-10-2018');
INSERT INTO test.quorum2 VALUES (2, '15-11-2018');

SET insert_quorum_timeout=100;

SYSTEM STOP FETCHES test.quorum1;

INSERT INTO test.quorum2 VALUES (1, '15-12-2018'); -- { serverError 319 } --

SELECT count(*) FROM test.quorum1;
SELECT count(*) FROM test.quorum2;

SET select_sequential_consistency=0;

SELECT count(*) FROM test.quorum2;

SET select_sequential_consistency=1;

SET insert_quorum_timeout=100;

SYSTEM START FETCHES test.quorum1;
SYSTEM SYNC REPLICA test.quorum1;
 
SELECT count(*) FROM test.quorum1;
SELECT count(*) FROM test.quorum2;

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;
