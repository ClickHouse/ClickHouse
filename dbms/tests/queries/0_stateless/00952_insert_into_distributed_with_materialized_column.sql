DROP TABLE IF EXISTS local_00952;
DROP TABLE IF EXISTS distributed_00952;

CREATE TABLE local_00952 (date Date, value Date MATERIALIZED toDate('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE distributed_00952 AS local_00952 ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_00952, rand());

SET insert_distributed_sync=1;

INSERT INTO distributed_00952 VALUES ('2018-08-01');
SELECT * FROM distributed_00952;
SELECT * FROM local_00952;
SELECT date, value FROM local_00952;

DROP TABLE distributed_00952;
DROP TABLE local_00952;
