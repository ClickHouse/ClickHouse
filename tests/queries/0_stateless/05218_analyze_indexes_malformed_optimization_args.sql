DROP TABLE IF EXISTS t_analyze_optimization_args;
CREATE TABLE t_analyze_optimization_args (key Int, v Array(Float32)) ENGINE = MergeTree ORDER BY key;

-- A malformed argument list of an optimization is rejected instead of being navigated into.

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', 42); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', 'not an array'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', NULL); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- The number of parameters is checked.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Every parameter is checked for its type, a mismatch is a `BAD_ARGUMENTS` error naming the parameter,
-- not an internal `BAD_GET`, for both the mixed-type `array(...)` call and a plain array literal.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array(1, 'L2Distance', 3, [1.5, 2.5, 3.5], 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', ['L2Distance'], 3, [1.5, 2.5, 3.5], 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', -3, [1.5, 2.5, 3.5], 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 'three', [1.5, 2.5, 3.5], 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, 1, 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, ['a', 'b'], 1, 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], 'yes', 0)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], 1, 2)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', ['v', 'L2Distance', 3, 1, 1, 0]); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', [1, 'L2Distance', 3, [1.0], 1, 0]); -- { serverError BAD_ARGUMENTS }

-- A well-formed argument list is accepted, in the shape `buildAnalyzeIndexQuery` sends (boolean flags),
-- with 0/1 flags and integer vector elements, and as a plain array literal; the table has no vector
-- similarity index, so the analysis returns no ranges.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1.5, 2.5, 3.5], true, false));
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', 3, [1, 2, 3], 1, 0));
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', ['v', 'L2Distance', 3, [1.5, 2.5, 3.5], 1, 0]);
-- A signed but non-negative limit is accepted as well.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_analyze_optimization_args, 1, [], 'vector_search_index_analysis', array('v', 'L2Distance', toInt64(3), [1.0], 1, 0));

DROP TABLE t_analyze_optimization_args;
