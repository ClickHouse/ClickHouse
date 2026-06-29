DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int, value Int) ENGINE = MergeTree() ORDER BY key;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', tuple(materialize(1))); -- { serverError BAD_ARGUMENTS }

DROP TABLE data;
