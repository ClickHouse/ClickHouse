DROP TABLE IF EXISTS t_analyze_optimization_args;
CREATE TABLE t_analyze_optimization_args (key Int, v Array(Float32)) ENGINE = MergeTree ORDER BY key;

-- A malformed argument list of an optimization is rejected instead of being navigated into.

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', 42); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', 'not an array'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', NULL); -- { serverError BAD_ARGUMENTS }

SET enable_analyzer = 0;
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', 42); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- The argument list `buildAnalyzeIndexQuery` sends is accepted on the old interpreter as well, where
-- each element keeps its own type instead of the whole array being coerced to one.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], 1, 0));
SET enable_analyzer = 1;

-- A well-formed argument list is accepted; the table has no vector similarity index, so the
-- analysis returns no ranges.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], 1, 0));

DROP TABLE t_analyze_optimization_args;
