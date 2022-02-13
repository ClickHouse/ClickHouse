-- Tags: no-fasttest

DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (resolution UInt8) ENGINE = Memory;

INSERT INTO table1 VALUES(0);
INSERT INTO table1 VALUES(1);
INSERT INTO table1 VALUES(2);
INSERT INTO table1 VALUES(3);
INSERT INTO table1 VALUES(4);
INSERT INTO table1 VALUES(5);
INSERT INTO table1 VALUES(6);
INSERT INTO table1 VALUES(7);
INSERT INTO table1 VALUES(8);
INSERT INTO table1 VALUES(9);
INSERT INTO table1 VALUES(10);
INSERT INTO table1 VALUES(11);
INSERT INTO table1 VALUES(12);
INSERT INTO table1 VALUES(13);
INSERT INTO table1 VALUES(14);
INSERT INTO table1 VALUES(15);


select h3GetPentagonIndexes(resolution) AS indexes from table1 order by indexes;
SELECT h3GetPentagonIndexes(20) AS indexes; -- { serverError 69 }

DROP TABLE table1
