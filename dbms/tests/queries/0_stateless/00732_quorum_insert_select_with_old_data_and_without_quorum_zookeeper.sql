SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;

CREATE TABLE test.quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE test.quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '2') ORDER BY x PARTITION BY y;

INSERT INTO test.quorum1 VALUES (1, '1990-11-15');
INSERT INTO test.quorum1 VALUES (2, '1990-11-15');
INSERT INTO test.quorum1 VALUES (3, '2020-12-16');

SYSTEM SYNC REPLICA test.quorum2;

SET select_sequential_consistency=1;
SET insert_quorum=2;

SET insert_quorum_timeout=0;

SYSTEM STOP FETCHES test.quorum1;

INSERT INTO test.quorum2 VALUES (4, toDate('2020-12-16')); -- { serverError 319 }

SELECT x FROM test.quorum1 ORDER BY x;
SELECT x FROM test.quorum2 ORDER BY x;

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;
