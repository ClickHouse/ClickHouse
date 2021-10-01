DROP TABLE IF EXISTS alter_00121;
CREATE TABLE alter_00121 (d Date, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_00121/t1', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01', 1);
ALTER TABLE alter_00121 DROP COLUMN x;

DROP TABLE alter_00121;

CREATE TABLE alter_00121 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_00121/t2', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01');
SELECT * FROM alter_00121 ORDER BY d;

ALTER TABLE alter_00121 ADD COLUMN x UInt8;

INSERT INTO alter_00121 VALUES ('2014-02-01', 1);
SELECT * FROM alter_00121 ORDER BY d;

ALTER TABLE alter_00121 DROP COLUMN x;
SELECT * FROM alter_00121 ORDER BY d;

DROP TABLE alter_00121;
