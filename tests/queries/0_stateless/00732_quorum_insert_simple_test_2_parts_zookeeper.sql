SET send_logs_level = 'none';

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;

CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/quorum', '2') ORDER BY x PARTITION BY y;

SET insert_quorum=2;
SET select_sequential_consistency=1;

INSERT INTO quorum1 VALUES (1, '2018-11-15');
INSERT INTO quorum1 VALUES (2, '2018-11-15');
INSERT INTO quorum1 VALUES (3, '2018-12-16');

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;
