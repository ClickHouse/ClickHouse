-- Tags: no-old-analyzer

-- The IEJoin planner path pre-sorts each input by its first-condition key with a plan-level
-- sort, which makes the inputs eligible for the read-in-order optimization: when a MergeTree
-- table is ordered by that key, the sort turns into a streaming FinishSorting (or is elided)
-- over an in-order read. Results must not depend on `optimize_read_in_order`.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS rio_l;
DROP TABLE IF EXISTS rio_r;
DROP TABLE IF EXISTS rio_bands;

CREATE TABLE rio_l (x Int64, y Int64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE rio_r (x Int64, y Int64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE rio_bands (lo Int64, hi Int64) ENGINE = MergeTree ORDER BY lo;

-- Two parts per table, so that the in-order read produces several sorted streams
-- that have to be merged.
INSERT INTO rio_l SELECT number, number % 97 FROM numbers(1500);
INSERT INTO rio_l SELECT number + 750, number % 89 FROM numbers(1500);
INSERT INTO rio_r SELECT number * 2, number % 83 FROM numbers(1000);
INSERT INTO rio_r SELECT number * 2 + 1, number % 79 FROM numbers(1000);
INSERT INTO rio_bands SELECT number * 5, number * 5 + 37 FROM numbers(400);
INSERT INTO rio_bands SELECT number * 7 + 3, number * 7 + 40 FROM numbers(400);

-- ASC-friendly operator pair: the first condition `<` keeps the L1 order ascending, so the
-- pre-sorted inputs are merged forward.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%IEJoin%';

-- Both inputs must be read in order.
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%Read type: InOrder%';

-- The result does not depend on how the input order was produced.
SELECT (
    SELECT (count(), sum(cityHash64(l.x, l.y, r.x, r.y))) FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y SETTINGS optimize_read_in_order = 1
) = (
    SELECT (count(), sum(cityHash64(l.x, l.y, r.x, r.y))) FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y SETTINGS optimize_read_in_order = 0
);

-- ... and matches the generic join executor.
SELECT (
    SELECT (count(), sum(cityHash64(l.x, l.y, r.x, r.y))) FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y
) = (
    SELECT (count(), sum(cityHash64(l.x, l.y, r.x, r.y))) FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

SELECT count() FROM rio_l l JOIN rio_r r ON l.x < r.x AND l.y > r.y;

-- BETWEEN desugars to `>=` + `<=`: the first condition `>=` makes the L1 order descending,
-- which is produced by iterating the forward-sorted (in-order read) inputs backwards.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%IEJoin%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%Read type: InOrder%';

SELECT (
    SELECT (count(), sum(cityHash64(a.x, a.y, b.lo, b.hi))) FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi SETTINGS optimize_read_in_order = 1
) = (
    SELECT (count(), sum(cityHash64(a.x, a.y, b.lo, b.hi))) FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi SETTINGS optimize_read_in_order = 0
);

SELECT (
    SELECT (count(), sum(cityHash64(a.x, a.y, b.lo, b.hi))) FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi
) = (
    SELECT (count(), sum(cityHash64(a.x, a.y, b.lo, b.hi))) FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

SELECT count() FROM rio_l a JOIN rio_bands b ON a.x BETWEEN b.lo AND b.hi;

DROP TABLE rio_l;
DROP TABLE rio_r;
DROP TABLE rio_bands;
