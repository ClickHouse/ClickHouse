-- Tags: no-old-analyzer

-- Shapes and storages the band join must decline even with `band_join` listed first: the
-- Join engine forces its own filled-join path, ANY strictness keeps the pre-existing error,
-- and OR-ladders / fewer-than-two-inequality shapes fall back to a cross join with a filter
-- (never a BandJoin step).

SET join_algorithm = 'band_join,ie_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS dp_p;
DROP TABLE IF EXISTS dp_i;
DROP TABLE IF EXISTS dp_sj;

CREATE TABLE dp_p (id Int32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dp_i (id Int32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO dp_p SELECT number, number % 23 FROM numbers(200);
INSERT INTO dp_i SELECT number, number % 17, number % 17 + 4 FROM numbers(200);

-- A table with the Join engine cannot serve the band shape: it is a prepared hash join over
-- its equality key, and joining it with anything else keeps the pre-existing errors.
CREATE TABLE dp_sj (lo Int64, hi Int64) ENGINE = Join(ALL, LEFT, lo);
INSERT INTO dp_sj VALUES (1, 5), (2, 8);

SELECT count() FROM dp_p p JOIN dp_sj ON p.t >= dp_sj.lo AND p.t <= dp_sj.hi; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT count() FROM dp_p p LEFT JOIN dp_sj ON p.t >= dp_sj.lo AND p.t <= dp_sj.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }
-- the control: the same table over its equality key works (the filled-join path)
SELECT 'storage join equality', count() FROM dp_p p LEFT JOIN dp_sj ON p.t = dp_sj.lo WHERE dp_sj.hi != 0;

-- ANY strictness with only inequality conditions keeps the pre-existing error
SELECT count() FROM dp_p p ANY INNER JOIN dp_i i ON p.t >= i.lo AND p.t <= i.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM dp_p p ANY LEFT JOIN dp_i i ON p.t >= i.lo AND p.t <= i.hi; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- An OR-ladder of band shapes is not a band (a single conjunction is required)
SELECT 'or-ladder', countIf(explain LIKE '%BandJoin%'), countIf(explain LIKE '%IEJoin%') FROM (
    EXPLAIN SELECT count() FROM dp_p p JOIN dp_i i ON (p.t >= i.lo AND p.t <= i.hi) OR (p.t + 100 >= i.lo AND p.t + 100 <= i.hi)
);
SELECT 'or-ladder result', (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p JOIN dp_i i ON (p.t >= i.lo AND p.t <= i.hi) OR (p.t + 100 >= i.lo AND p.t + 100 <= i.hi)
) = (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p, dp_i i WHERE (p.t >= i.lo AND p.t <= i.hi) OR (p.t + 100 >= i.lo AND p.t + 100 <= i.hi)
) AS ok;

-- A single inequality is not a band (IEJoin also needs two, so no specialized step at all)
SELECT 'single inequality', countIf(explain LIKE '%BandJoin%'), countIf(explain LIKE '%IEJoin%') FROM (
    EXPLAIN SELECT count() FROM dp_p p JOIN dp_i i ON p.t >= i.lo
);
SELECT 'single inequality result', (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p JOIN dp_i i ON p.t >= i.lo
) = (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p, dp_i i WHERE p.t >= i.lo
) AS ok;

-- A one-sided second conjunct leaves one usable inequality: still not a band
SELECT 'one-sided conjunct', countIf(explain LIKE '%BandJoin%'), countIf(explain LIKE '%IEJoin%') FROM (
    EXPLAIN SELECT count() FROM dp_p p JOIN dp_i i ON p.t >= i.lo AND p.t <= 100
);
SELECT 'one-sided conjunct result', (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p JOIN dp_i i ON p.t >= i.lo AND p.t <= 100
) = (
    SELECT arraySort(groupArray((p.id, i.id))) FROM dp_p p, dp_i i WHERE p.t >= i.lo AND p.t <= 100
) AS ok;

DROP TABLE dp_p;
DROP TABLE dp_i;
DROP TABLE dp_sj;
