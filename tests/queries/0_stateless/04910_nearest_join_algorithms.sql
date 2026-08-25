-- NEAREST JOIN works with the hash and parallel_hash algorithms and rejects the others.

DROP TABLE IF EXISTS alg_upload;
DROP TABLE IF EXISTS alg_base;

CREATE TABLE alg_base (k UInt32, base_id UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO alg_base
SELECT
    number % 50 AS k,
    number AS base_id,
    arrayMap(i -> toFloat64(sipHash64(number, i) % 1000000) / 1000000, range(4)) AS vec
FROM numbers(5000);

CREATE TABLE alg_upload (query_id UInt32, k UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY query_id;
INSERT INTO alg_upload
SELECT
    number AS query_id,
    number % 50 AS k,
    arrayMap(i -> toFloat64(sipHash64(number, i, 42) % 1000000) / 1000000, range(4)) AS vec
FROM numbers(1000);

SELECT 'hash';
SELECT sum(cityHash64(query_id, base_id)) FROM
(
    SELECT upload.query_id AS query_id, base.base_id AS base_id
    FROM alg_upload AS upload
    NEAREST JOIN alg_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS join_algorithm = 'hash';

SELECT 'parallel_hash';
SELECT sum(cityHash64(query_id, base_id)) FROM
(
    SELECT upload.query_id AS query_id, base.base_id AS base_id
    FROM alg_upload AS upload
    NEAREST JOIN alg_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1;

SELECT 'grace_hash is rejected';
SELECT count() FROM
(
    SELECT upload.query_id
    FROM alg_upload AS upload
    NEAREST JOIN alg_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS join_algorithm = 'grace_hash'; -- { serverError NOT_IMPLEMENTED }

SELECT 'full_sorting_merge is rejected';
SELECT count() FROM
(
    SELECT upload.query_id
    FROM alg_upload AS upload
    NEAREST JOIN alg_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE alg_upload;
DROP TABLE alg_base;
