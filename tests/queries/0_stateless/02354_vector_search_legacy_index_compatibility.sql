-- Indexes of type 'annoy' or 'usearch' are no longer supported.
-- Test what happens when ClickHouse encounters tables with the old index type.

DROP TABLE IF EXISTS tab;

SELECT 'Annoy';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX vec_idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]); -- { serverError ILLEGAL_INDEX }

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;
-- (*) The search succeeds because the index contains no data (i.e. some shortcut)
-- If it had data (can't really test in SQL tests ...), this statement would also return an error, trust me.

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

DROP TABLE tab;

SELECT 'Usearch';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX vec_idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]); -- { serverError ILLEGAL_INDEX }

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;
-- see above: (*)

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

DROP TABLE tab;
