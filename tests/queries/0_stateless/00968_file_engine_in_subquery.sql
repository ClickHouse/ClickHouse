DROP TABLE IF EXISTS tableFile_00968;
DROP TABLE IF EXISTS tableMergeTree_00968;
CREATE TABLE tableFile_00968(number UInt64) ENGINE = File('TSV');
CREATE TABLE tableMergeTree_00968(id UInt64) ENGINE = MergeTree() PARTITION BY id ORDER BY id;

INSERT INTO tableFile_00968 SELECT number FROM system.numbers LIMIT 10;
INSERT INTO tableMergeTree_00968 SELECT number FROM system.numbers LIMIT 100;

SELECT id FROM tableMergeTree_00968 WHERE id IN (SELECT number FROM tableFile_00968) ORDER BY id;

DROP TABLE tableFile_00968;
DROP TABLE tableMergeTree_00968;
