-- Tags: no-old-analyzer

-- Detection of the band shape (`t {>,>=} lo AND t {<,<=} hi` with a shared point-side
-- expression) and the list-position priority of `band_join` relative to `ie_join` and the
-- equality-based algorithms.

-- Pin the setting (it is randomized in tests): with `band_join` ahead in the list the
-- runtime-filter pass must leave the join alone instead of pinning it to a hash-family
-- algorithm.
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS det_p;
DROP TABLE IF EXISTS det_i;

CREATE TABLE det_p (id Int32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE det_i (id Int32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO det_p SELECT number, number % 20 FROM numbers(100);
INSERT INTO det_i SELECT number, number % 15, number % 15 + 3 FROM numbers(100);

-- Keep the written join order so the EXPLAIN pins below see the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,ie_join,hash';

SELECT 'band shape', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';
SELECT 'point side line', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%PointSide: Left%';
SELECT 'band count', count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi;

-- All four strict/loose bracket combinations and the reversed spelling of the bounds detect
SELECT 'strict brackets', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t > i.lo AND p.t < i.hi) WHERE explain LIKE '%BandJoin%';
SELECT 'reversed spelling', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON i.lo <= p.t AND i.hi >= p.t) WHERE explain LIKE '%BandJoin%';
SELECT 'between', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t BETWEEN i.lo AND i.hi) WHERE explain LIKE '%BandJoin%';

-- A non-band two-inequality shape (different point-side keys) falls through to IEJoin
SELECT 'non-band to iejoin', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t < i.lo AND p.id > i.id);

-- Same-direction bounds over the shared key are not a band
SELECT 'same direction', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t >= i.hi);

-- The point side on the right is executed as the swapped mirror (details in 04569)
SELECT 'right orientation', countIf(explain LIKE '%BandJoin%') > 0
FROM (EXPLAIN SELECT count() FROM det_i i JOIN det_p p ON p.t >= i.lo AND p.t <= i.hi);

-- Non-INNER kinds are out of scope for now and fall through to IEJoin
SELECT 'left kind', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM det_p p LEFT JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi);

-- `ie_join` listed first claims the join even for the band shape
SET join_algorithm = 'ie_join,band_join,hash';
SELECT 'ie_join first', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi);

-- Listed after hash, `band_join` takes only joins hash cannot execute (no equality conditions)
SET join_algorithm = 'hash,band_join';
SELECT 'hash first, no equality', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';
SELECT 'hash first, equality', count() FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.id = i.id AND p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

-- Listed first, `band_join` claims the join and the equality becomes a filter over the result
SET join_algorithm = 'band_join,hash';
SELECT 'band first, equality', count() > 0 FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.id = i.id AND p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';
SELECT 'band first, equality parity', (
    SELECT arraySort(groupArray((p.id, i.id))) FROM det_p p JOIN det_i i ON p.id = i.id AND p.t >= i.lo AND p.t <= i.hi
) = (
    SELECT arraySort(groupArray((p.id, i.id))) FROM det_p p JOIN det_i i ON p.id = i.id AND p.t >= i.lo AND p.t <= i.hi
    SETTINGS join_algorithm = 'hash'
);

-- Without `band_join` in the list the step never appears
SET join_algorithm = 'ie_join,hash';
SELECT 'not listed', count() FROM (EXPLAIN SELECT count() FROM det_p p JOIN det_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

DROP TABLE det_p;
DROP TABLE det_i;
