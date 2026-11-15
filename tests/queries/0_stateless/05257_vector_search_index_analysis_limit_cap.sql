-- Tags: no-fasttest

-- Tests that `mergeTreeAnalyzeIndexes` rejects a 'vector_search_index_analysis' neighbour count above
-- `max_limit_for_vector_search_queries`, the bound under which a regular vector search query may use the index.
-- Such a count used to be handed to the vector similarity index unchanged, which then reserved memory
-- proportional to it (32 GiB for the count below) instead of to the number of rows in the index.

DROP TABLE IF EXISTS t_vector_index_analysis_limit;

CREATE TABLE t_vector_index_analysis_limit
(
    id UInt32,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO t_vector_index_analysis_limit SELECT number, [number / 100, number / 100] FROM numbers(100);

-- `additional filters present` (the 5th argument) is false and `vector_search_with_rescoring` is left
-- at its default: either one engages a separate, pre-existing bound on the neighbour count, which would
-- hide a missing bound here.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 2147483648, [0.3, 0.3], false, false)); -- { serverError BAD_ARGUMENTS }

-- A neighbour count within the bound is still analyzed, and the index still narrows the part.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false));

-- The bound is the setting, and it is inclusive: the planner uses the index at a LIMIT equal to the
-- setting, so this function must accept exactly that.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false))
SETTINGS max_limit_for_vector_search_queries = 4;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false))
SETTINGS max_limit_for_vector_search_queries = 3; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_vector_index_analysis_limit;
