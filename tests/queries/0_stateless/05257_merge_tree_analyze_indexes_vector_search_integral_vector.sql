-- Tags: no-fasttest
-- An integral search vector with a negative component, as distributed index analysis sends it,
-- must select the same granules as its floating-point spelling.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, [number - 50, 0] FROM numbers(100);

SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), tab, true, [], 'vector_search_index_analysis', array('vec', 'L2Distance', 3, [-20, 0], false, true)) ORDER BY part_name;
SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), tab, true, [], 'vector_search_index_analysis', array('vec', 'L2Distance', 3, [-20.0, 0.0], false, true)) ORDER BY part_name;

DROP TABLE tab;
