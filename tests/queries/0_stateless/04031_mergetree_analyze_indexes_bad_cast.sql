DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO data VALUES (1);

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, materialize(0), array('all_1_1_0', toUInt128(9))); -- { serverError BAD_ARGUMENTS }

DROP TABLE data;
