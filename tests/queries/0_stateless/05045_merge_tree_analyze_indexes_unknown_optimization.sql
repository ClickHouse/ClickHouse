-- Unknown optimization names in `mergeTreeAnalyzeIndexes` must be rejected with `BAD_ARGUMENTS`.
DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO data VALUES (1);

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1);
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1, [], '', 123); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1 = 1, [], 'bogus_optimization', [1, 2, 3]); -- { serverError BAD_ARGUMENTS }

DROP TABLE data;
