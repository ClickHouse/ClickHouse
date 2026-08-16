-- NEAREST JOIN must return exactly the rows of the equivalent
-- "equality join, then take the row with the minimal distance per left row" rewrite.

DROP TABLE IF EXISTS eq_upload;
DROP TABLE IF EXISTS eq_base;

CREATE TABLE eq_base (k UInt32, base_id UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY k;
-- Deterministic pseudo-random vectors. Distances between distinct rows are distinct with
-- overwhelming probability, so the nearest row is unambiguous.
INSERT INTO eq_base
SELECT
    number % 100 AS k,
    number AS base_id,
    arrayMap(i -> toFloat64(sipHash64(number, i) % 1000000) / 1000000, range(8)) AS vec
FROM numbers(10000);

CREATE TABLE eq_upload (query_id UInt32, k UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY query_id;
-- k = 100..119 have no match in eq_base.
INSERT INTO eq_upload
SELECT
    number AS query_id,
    number % 120 AS k,
    arrayMap(i -> toFloat64(sipHash64(number, i, 42) % 1000000) / 1000000, range(8)) AS vec
FROM numbers(3000);

SELECT 'inner: rows differing from the LIMIT 1 BY rewrite in both directions (expect 0 0)';
WITH
    nearest_result AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM eq_upload AS upload
        NEAREST JOIN eq_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
    ),
    rewrite_result AS
    (
        SELECT query_id, base_id FROM
        (
            SELECT upload.query_id AS query_id, base.base_id AS base_id
            FROM eq_upload AS upload
            INNER JOIN eq_base AS base ON upload.k = base.k
            ORDER BY upload.query_id, L2Distance(upload.vec, base.vec)
            LIMIT 1 BY upload.query_id
        )
    )
SELECT
    (SELECT count() FROM (SELECT * FROM nearest_result EXCEPT SELECT * FROM rewrite_result)),
    (SELECT count() FROM (SELECT * FROM rewrite_result EXCEPT SELECT * FROM nearest_result));

SELECT 'inner: row count matches the number of matched left rows (expect 2500)';
SELECT count()
FROM eq_upload AS upload
NEAREST JOIN eq_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec);

SELECT 'cosine: rows differing from the LIMIT 1 BY rewrite in both directions (expect 0 0)';
WITH
    nearest_result AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM eq_upload AS upload
        NEAREST JOIN eq_base AS base ON upload.k = base.k AND cosineDistance(upload.vec, base.vec)
    ),
    rewrite_result AS
    (
        SELECT query_id, base_id FROM
        (
            SELECT upload.query_id AS query_id, base.base_id AS base_id
            FROM eq_upload AS upload
            INNER JOIN eq_base AS base ON upload.k = base.k
            ORDER BY upload.query_id, cosineDistance(upload.vec, base.vec)
            LIMIT 1 BY upload.query_id
        )
    )
SELECT
    (SELECT count() FROM (SELECT * FROM nearest_result EXCEPT SELECT * FROM rewrite_result)),
    (SELECT count() FROM (SELECT * FROM rewrite_result EXCEPT SELECT * FROM nearest_result));

SELECT 'left: unmatched left rows are preserved with defaults (expect 500)';
SELECT countIf(base.base_id = 0 AND empty(base.vec))
FROM eq_upload AS upload
NEAREST LEFT JOIN eq_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
WHERE upload.k >= 100;

DROP TABLE eq_upload;
DROP TABLE eq_base;
