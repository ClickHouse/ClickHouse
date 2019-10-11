DROP TABLE IF EXISTS alter_00121;
CREATE TABLE alter_00121 (d Date, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_00121', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01', 1);
ALTER TABLE alter_00121 DROP COLUMN x;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter_00121/replicas/r1/parts/20140101_20140101_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE alter_00121;


CREATE TABLE alter_00121 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_00121', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01');
SELECT * FROM alter_00121 ORDER BY d;

ALTER TABLE alter_00121 ADD COLUMN x UInt8;

INSERT INTO alter_00121 VALUES ('2014-02-01', 1);
SELECT * FROM alter_00121 ORDER BY d;

ALTER TABLE alter_00121 DROP COLUMN x;
SELECT * FROM alter_00121 ORDER BY d;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/test/alter_00121/replicas/r1/parts/20140201_20140201_0_0_0' AND name = 'columns' FORMAT TabSeparatedRaw;

DROP TABLE alter_00121;
