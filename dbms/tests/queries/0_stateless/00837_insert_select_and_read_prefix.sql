DROP TABLE IF EXISTS test.file;
CREATE TABLE test.file (s String, n UInt32) ENGINE = File(CSVWithNames);
-- BTW, WithNames formats are totally unsuitable for more than a single INSERT
INSERT INTO test.file VALUES ('hello', 1), ('world', 2);

SELECT * FROM test.file;
CREATE TEMPORARY TABLE file2 AS SELECT * FROM test.file;
SELECT * FROM file2;

DROP TABLE test.file;
