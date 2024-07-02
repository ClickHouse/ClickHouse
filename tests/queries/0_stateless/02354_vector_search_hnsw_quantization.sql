-- Tags: no-fasttest, no-ordinary-database

-- Tests queries utilizing vector similarity indexes with different quantization-s.

SET allow_experimental_usearch_index = 1;
SET allow_experimental_analyzer = 0;

-- Not a systematic test, just to check that no bad things happen.

-- Most tests are commented out because usearch currently goes boom when they are enabled.

SELECT '-- f32';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 5, 5, 5));
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;

SELECT '-- f16';

CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f16', 5, 5, 5));
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;

-- SELECT '-- f8';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f8', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- u64';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'u64', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;

-- SELECT '-- u32';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'u32', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- u16';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'u16', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- u8';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'u8', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- i64';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i64', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;

-- SELECT '-- i32';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i32', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- i16';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i16', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
SELECT '-- i8';

CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i8', 5, 5, 5));
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;
--
-- SELECT '-- b1x8';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'b1x8', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
--
-- SELECT '-- u40';
--
-- CREATE TABLE tab(id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'u40', 5, 5, 5));
-- INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
--
-- WITH [0.0, 2.0] AS reference_vec
-- SELECT id, vec, L2Distance(vec, reference_vec)
-- FROM tab
-- ORDER BY L2Distance(vec, reference_vec)
-- LIMIT 3;
--
-- DROP TABLE tab;
