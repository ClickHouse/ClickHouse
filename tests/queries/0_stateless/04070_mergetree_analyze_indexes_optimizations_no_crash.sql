DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int, value Int) ENGINE = MergeTree() ORDER BY key;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', tuple(materialize(1))); -- { serverError BAD_ARGUMENTS }

-- A parts argument that is not an array of strings. The array literal and the `array(...)` call are
-- extracted by different branches, and a literal of another type does not reach the elements at all.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [1]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, array(1)); -- { serverError BAD_ARGUMENTS }

-- `DESCRIBE TABLE` parses the arguments before the analyzer resolves them, so it reaches the same
-- code with the argument as written, including a `_CAST` the analyzer would have rejected first.
DESCRIBE TABLE mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, array(1)); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST()); -- { serverError BAD_ARGUMENTS }

-- A well-formed parts argument is still accepted in both spellings, so a rejection of every array
-- would be reported here rather than passing. `data` has no parts, so nothing is returned.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, ['all_1_1_0']);
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, array('all_1_1_0'));

DROP TABLE data;
