DROP TABLE IF EXISTS test.alter;
CREATE TABLE test.alter (d Date, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r1', d, (d), 8192);

INSERT INTO test.alter VALUES ('2014-01-01', 1);
ALTER TABLE test.alter DROP COLUMN x;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter/replicas/r1/parts/20140101_20140101_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE test.alter;


CREATE TABLE test.alter (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r1', d, (d), 8192);

INSERT INTO test.alter VALUES ('2014-01-01');
SELECT * FROM test.alter ORDER BY d;

ALTER TABLE test.alter ADD COLUMN x UInt8;

INSERT INTO test.alter VALUES ('2014-02-01', 1);
SELECT * FROM test.alter ORDER BY d;

ALTER TABLE test.alter DROP COLUMN x;
SELECT * FROM test.alter ORDER BY d;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter/replicas/r1/parts/20140201_20140201_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE test.alter;
