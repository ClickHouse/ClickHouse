-- Tags: long, no-old-analyzer

-- `x BETWEEN a AND b` shapes: when both left keys are the same column and the operator
-- families are opposite (exactly then the L1 and L2 directions coincide for the left side),
-- the operator builds L2 by merging the already-ordered left entries with the sorted right
-- entries instead of sorting the whole union. All variants are verified against the
-- cross-join oracle; the same-family and mixed-type variants must take the general L2 path
-- and stay correct.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 128;

DROP TABLE IF EXISTS pts;
DROP TABLE IF EXISTS bands;
DROP TABLE IF EXISTS pts32;
DROP TABLE IF EXISTS bands_mixed;

CREATE TABLE pts (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bands (id Int64, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
-- Duplicate-heavy point values and overlapping bands.
INSERT INTO pts SELECT number, intDiv(number, 40) FROM numbers(1000);
INSERT INTO bands SELECT number, number % 20, number % 20 + intDiv(number, 100) FROM numbers(800);

-- The comparisons below are vacuous if the JOIN side is not routed through IEJoin: pin the plan.
SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM pts p JOIN bands b ON p.x BETWEEN b.lo AND b.hi) WHERE explain LIKE '%IEJoin%';

-- BETWEEN desugars to `>=` + `<=`: opposite families, merge shortcut.
SELECT 'between', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p JOIN bands b ON p.x BETWEEN b.lo AND b.hi) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p, bands b WHERE p.x BETWEEN b.lo AND b.hi) AS ok, (SELECT count() FROM pts p JOIN bands b ON p.x BETWEEN b.lo AND b.hi) AS cnt;

-- Opposite-family strict pair (shortcut without BETWEEN).
SELECT '>  <', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p JOIN bands b ON p.x > b.lo AND p.x < b.hi) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p, bands b WHERE p.x > b.lo AND p.x < b.hi) AS ok, (SELECT count() FROM pts p JOIN bands b ON p.x > b.lo AND p.x < b.hi) AS cnt;

-- The same BETWEEN with the `<=` condition first: still opposite families, L1 ascending.
SELECT '<= >=', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p JOIN bands b ON p.x <= b.hi AND p.x >= b.lo) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p, bands b WHERE p.x <= b.hi AND p.x >= b.lo) AS ok, (SELECT count() FROM pts p JOIN bands b ON p.x <= b.hi AND p.x >= b.lo) AS cnt;

-- Same-family pairs: must take the general L2 path and stay correct.
SELECT '>= >=', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p JOIN bands b ON p.x >= b.lo AND p.x >= b.hi) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p, bands b WHERE p.x >= b.lo AND p.x >= b.hi) AS ok, (SELECT count() FROM pts p JOIN bands b ON p.x >= b.lo AND p.x >= b.hi) AS cnt;
SELECT '<  <', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p JOIN bands b ON p.x < b.lo AND p.x < b.hi) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts p, bands b WHERE p.x < b.lo AND p.x < b.hi) AS ok, (SELECT count() FROM pts p JOIN bands b ON p.x < b.lo AND p.x < b.hi) AS cnt;

-- Mixed key types: the left column is cast to different common types per condition, so the
-- two left key positions differ and the general path is taken.
CREATE TABLE pts32 (id Int64, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bands_mixed (id Int64, lo Int32, hi Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pts32 SELECT number, toInt32(intDiv(number, 40)) FROM numbers(1000);
INSERT INTO bands_mixed SELECT number, toInt32(number % 20), toInt64(number % 20 + intDiv(number, 100)) FROM numbers(800);

SELECT 'plan mixed', count() > 0 FROM (EXPLAIN SELECT count() FROM pts32 p JOIN bands_mixed b ON p.x BETWEEN b.lo AND b.hi) WHERE explain LIKE '%IEJoin%';
SELECT 'mixed', (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts32 p JOIN bands_mixed b ON p.x BETWEEN b.lo AND b.hi) = (SELECT (count(), sum(cityHash64(p.id, b.id))) FROM pts32 p, bands_mixed b WHERE p.x BETWEEN b.lo AND b.hi) AS ok, (SELECT count() FROM pts32 p JOIN bands_mixed b ON p.x BETWEEN b.lo AND b.hi) AS cnt;

DROP TABLE pts;
DROP TABLE bands;
DROP TABLE pts32;
DROP TABLE bands_mixed;
