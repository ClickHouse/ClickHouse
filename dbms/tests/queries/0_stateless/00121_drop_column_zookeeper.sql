DROP TABLE IF EXISTS alter;
CREATE TABLE alter (d Date, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r1', d, (d), 8192);

INSERT INTO alter VALUES ('2014-01-01', 1);
ALTER TABLE alter DROP COLUMN x;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter/replicas/r1/parts/20140101_20140101_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE alter;


CREATE TABLE alter (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r1', d, (d), 8192);

INSERT INTO alter VALUES ('2014-01-01');
SELECT * FROM alter ORDER BY d;

ALTER TABLE alter ADD COLUMN x UInt8;

INSERT INTO alter VALUES ('2014-02-01', 1);
SELECT * FROM alter ORDER BY d;

ALTER TABLE alter DROP COLUMN x;
SELECT * FROM alter ORDER BY d;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter/replicas/r1/parts/20140201_20140201_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE alter;
