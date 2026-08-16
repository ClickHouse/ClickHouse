-- The join order optimizer may swap the inputs of an INNER NEAREST join so that the smaller
-- (original left) side becomes the build side. The result must not depend on the swap.

DROP TABLE IF EXISTS swap_upload;
DROP TABLE IF EXISTS swap_base;

CREATE TABLE swap_base (k UInt32, base_id UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO swap_base
SELECT
    number % 100 AS k,
    number AS base_id,
    arrayMap(i -> toFloat64(sipHash64(number, i) % 1000000) / 1000000, range(8)) AS vec
FROM numbers(50000);

CREATE TABLE swap_upload (query_id UInt32, k UInt32, vec Array(Float64)) ENGINE = MergeTree ORDER BY query_id;
-- k = 100..119 have no match in swap_base.
INSERT INTO swap_upload
SELECT
    number AS query_id,
    number % 120 AS k,
    arrayMap(i -> toFloat64(sipHash64(number, i, 42) % 1000000) / 1000000, range(8)) AS vec
FROM numbers(1000);

SELECT 'L2: rows differing between the swapped and unswapped plans (expect 0 0)';
WITH
    unswapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 0
    ),
    swapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 1
    )
SELECT
    (SELECT count() FROM (SELECT * FROM unswapped EXCEPT SELECT * FROM swapped)),
    (SELECT count() FROM (SELECT * FROM swapped EXCEPT SELECT * FROM unswapped));

SELECT 'L2: the swapped plan matches every matchable row (expect 840)';
SELECT count() FROM
(
    SELECT upload.query_id
    FROM swap_upload AS upload
    NEAREST JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS query_plan_join_swap_table = 1;

SELECT 'cosine: rows differing between the swapped and unswapped plans (expect 0 0)';
WITH
    unswapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND cosineDistance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 0
    ),
    swapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND cosineDistance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 1
    )
SELECT
    (SELECT count() FROM (SELECT * FROM unswapped EXCEPT SELECT * FROM swapped)),
    (SELECT count() FROM (SELECT * FROM swapped EXCEPT SELECT * FROM unswapped));

SELECT 'the distance is recomputable from the output columns under swap (expect 0)';
WITH
    unswapped AS
    (
        SELECT upload.query_id AS query_id, round(L2Distance(upload.vec, base.vec), 6) AS d
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 0
    ),
    swapped AS
    (
        SELECT upload.query_id AS query_id, round(L2Distance(upload.vec, base.vec), 6) AS d
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
        SETTINGS query_plan_join_swap_table = 1
    )
SELECT count() FROM (SELECT * FROM unswapped EXCEPT SELECT * FROM swapped);

SELECT 'a single-side ON condition filters candidates identically under swap (expect 0 0)';
WITH
    unswapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base
            ON upload.k = base.k AND L2Distance(upload.vec, base.vec) AND base.base_id % 7 != 0
        SETTINGS query_plan_join_swap_table = 0
    ),
    swapped AS
    (
        SELECT upload.query_id AS query_id, base.base_id AS base_id
        FROM swap_upload AS upload
        NEAREST JOIN swap_base AS base
            ON upload.k = base.k AND L2Distance(upload.vec, base.vec) AND base.base_id % 7 != 0
        SETTINGS query_plan_join_swap_table = 1
    )
SELECT
    (SELECT count() FROM (SELECT * FROM unswapped EXCEPT SELECT * FROM swapped)),
    (SELECT count() FROM (SELECT * FROM swapped EXCEPT SELECT * FROM unswapped));

SELECT 'LEFT NEAREST is never swapped and keeps unmatched rows (expect 1000 160)';
SELECT count(), countIf(base_id = 0 AND empty(vec_out)) FROM
(
    SELECT upload.query_id AS query_id, base.base_id AS base_id, base.vec AS vec_out
    FROM swap_upload AS upload
    NEAREST LEFT JOIN swap_base AS base ON upload.k = base.k AND L2Distance(upload.vec, base.vec)
)
SETTINGS query_plan_join_swap_table = 1;

DROP TABLE swap_upload;
DROP TABLE swap_base;
