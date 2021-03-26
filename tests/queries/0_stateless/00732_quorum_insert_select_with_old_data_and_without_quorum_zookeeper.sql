SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;

CREATE TABLE quorum1(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00732/quorum_old_data', '1') ORDER BY x PARTITION BY y;
CREATE TABLE quorum2(x UInt32, y Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00732/quorum_old_data', '2') ORDER BY x PARTITION BY y;

INSERT INTO quorum1 VALUES (1, '1990-11-15');
INSERT INTO quorum1 VALUES (2, '1990-11-15');
INSERT INTO quorum1 VALUES (3, '2020-12-16');

SYSTEM SYNC REPLICA quorum2;

SET select_sequential_consistency=1;
SET insert_quorum=2;

SET insert_quorum_timeout=0;

SYSTEM STOP FETCHES quorum1;

INSERT INTO quorum2 VALUES (4, toDate('2020-12-16')); -- { serverError 319 }

SELECT x FROM quorum1 ORDER BY x;
SELECT x FROM quorum2 ORDER BY x;

DROP TABLE quorum1;
DROP TABLE quorum2;
