SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;

CREATE TABLE test.quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE test.quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '2') ORDER BY x PARTITION BY y;

SET insert_quorum=2;
SET select_sequential_consistency=1;

INSERT INTO test.quorum1 VALUES (1, toDate('2018-11-15'));
INSERT INTO test.quorum1 VALUES (2, toDate('2018-11-15'));

SELECT count(*) FROM test.quorum1;
SELECT count(*) FROM test.quorum2;

DROP TABLE IF EXISTS test.quorum1;
DROP TABLE IF EXISTS test.quorum2;
