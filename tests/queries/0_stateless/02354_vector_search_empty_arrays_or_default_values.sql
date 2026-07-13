-- Tags: no-fasttest, no-ordinary-database

-- Vector similarity indexes must reject empty Arrays or Arrays with default values (issue #52258)

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 1)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, []); -- { serverError INCORRECT_DATA }
INSERT INTO tab (id) VALUES (1); -- { serverError INCORRECT_DATA }

DROP TABLE tab;
