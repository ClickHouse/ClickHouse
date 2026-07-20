-- Tags: no-old-analyzer

-- NULL and NaN keys on both sides of the band join: a NULL fails every inequality and a NaN
-- fails every IEEE comparison, so such rows must match nothing - on the point side and on
-- the interval side (in `lo`, in `hi`, or in both). Verified against the cross-join oracle
-- (same NULL/NaN semantics in a WHERE filter) and against `ie_join`.

-- Keep the written join order: the band join detects only the point-side-on-the-left
-- orientation for now, so a planner swap would silently change the executed algorithm.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS nn_p;
DROP TABLE IF EXISTS nn_i;

CREATE TABLE nn_p (id UInt32, t Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE nn_i (id UInt32, lo Nullable(Int64), hi Nullable(Int64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO nn_p SELECT number, if(number % 5 = 0, NULL, number % 30) FROM numbers(300);
INSERT INTO nn_i SELECT number,
    if(number % 7 = 0, NULL, number % 25),
    if(number % 11 = 0, NULL, number % 25 + 4)
FROM numbers(300);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM nn_p p JOIN nn_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

SELECT 'nullable int',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_p p JOIN nn_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_p p, nn_i i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_p p JOIN nn_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_p p JOIN nn_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM nn_p p JOIN nn_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- An interval side whose every row has a NULL key yields an empty result, not an error
SELECT 'all null intervals', count()
FROM nn_p p JOIN (SELECT id, CAST(NULL AS Nullable(Int64)) AS lo, hi FROM nn_i) i ON p.t >= i.lo AND p.t <= i.hi;

DROP TABLE nn_p;
DROP TABLE nn_i;

-- Floats: NaN in the point key, in `lo`, and in `hi`; infinities are ordinary values.
DROP TABLE IF EXISTS nn_fp;
DROP TABLE IF EXISTS nn_fi;

CREATE TABLE nn_fp (id UInt32, t Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE nn_fi (id UInt32, lo Float64, hi Float64) ENGINE = MergeTree ORDER BY id;

INSERT INTO nn_fp SELECT number, multiIf(number % 9 = 0, nan, number % 13 = 0, inf, number % 17 = 0, -inf, (number % 40) / 2.0) FROM numbers(300);
INSERT INTO nn_fi SELECT number,
    multiIf(number % 8 = 0, nan, number % 19 = 0, -inf, (number % 35) / 2.0),
    multiIf(number % 12 = 0, nan, number % 23 = 0, inf, (number % 35) / 2.0 + 3)
FROM numbers(300);

SELECT 'float nan inf',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p JOIN nn_fi i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p, nn_fi i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p JOIN nn_fi i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p JOIN nn_fi i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM nn_fp p JOIN nn_fi i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- Strict brackets exercise the other strictness paths under the same NULL/NaN rules
SELECT 'float strict',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p JOIN nn_fi i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM nn_fp p, nn_fi i WHERE p.t > i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT count() FROM nn_fp p JOIN nn_fi i ON p.t > i.lo AND p.t < i.hi) AS cnt;

-- Nullable floats combine both exclusion reasons in one column
SELECT 'nullable float',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, if(id % 4 = 0, NULL, t) AS t FROM nn_fp) p
     JOIN (SELECT id, if(id % 5 = 0, NULL, lo) AS lo, hi FROM nn_fi) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, if(id % 4 = 0, NULL, t) AS t FROM nn_fp) p, (SELECT id, if(id % 5 = 0, NULL, lo) AS lo, hi FROM nn_fi) i
           WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok;

DROP TABLE nn_fp;
DROP TABLE nn_fi;
