-- Tags: no-fasttest, no-ordinary-database

SET allow_experimental_vector_similarity_index = 1;

SET parallel_replicas_local_plan=1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas
SET allow_experimental_codecs=1;

DROP TABLE IF EXISTS tab_wide_full;

CREATE TABLE tab_wide_full(id Int32, vec Array(Float32) CODEC(SZ3)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 3;

INSERT INTO tab_wide_full VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

SELECT 'Check part formats';

SELECT * FROM tab_wide_full ORDER BY ALL;
DROP TABLE tab_wide_full;
