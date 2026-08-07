-- Tags: long, no-old-analyzer

-- Long runs of equal first-condition keys crossing block boundaries: pins the tie policy of
-- the merge that builds the L1 order from pre-sorted inputs (a loose first condition pulls the
-- left entry first, a strict one the right entry). Verified against the cross-join oracle
-- (comma join with the conditions in WHERE; `cross_to_inner_join_rewrite = 0` keeps it out of
-- IEJoin).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
-- Equal-key runs of 400 rows cross the 128-row blocks of the plan-level sort output.
SET max_block_size = 128;

DROP TABLE IF EXISTS ties_l;
DROP TABLE IF EXISTS ties_r;
DROP TABLE IF EXISTS ties_const_l;
DROP TABLE IF EXISTS ties_const_r;

CREATE TABLE ties_l (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ties_r (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ties_l SELECT number, intDiv(number, 400), number % 7 FROM numbers(1200);
INSERT INTO ties_r SELECT number, intDiv(number, 400), number % 5 FROM numbers(1200);

-- The comparisons below are vacuous if the JOIN side is not routed through IEJoin: pin the plan.
SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM ties_l l JOIN ties_r r ON l.x < r.x AND l.y >= r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'plan self', count() > 0 FROM (EXPLAIN SELECT count() FROM ties_l s1 JOIN ties_l s2 ON s1.x < s2.x AND s1.y > s2.y) WHERE explain LIKE '%IEJoin%';

-- Strict and loose variants of the first condition, in both directions.
SELECT '<  >=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l JOIN ties_r r ON l.x < r.x AND l.y >= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l, ties_r r WHERE l.x < r.x AND l.y >= r.y) AS ok, (SELECT count() FROM ties_l l JOIN ties_r r ON l.x < r.x AND l.y >= r.y) AS cnt;
SELECT '<= >', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l JOIN ties_r r ON l.x <= r.x AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l, ties_r r WHERE l.x <= r.x AND l.y > r.y) AS ok, (SELECT count() FROM ties_l l JOIN ties_r r ON l.x <= r.x AND l.y > r.y) AS cnt;
SELECT '>  <=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l JOIN ties_r r ON l.x > r.x AND l.y <= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l, ties_r r WHERE l.x > r.x AND l.y <= r.y) AS ok, (SELECT count() FROM ties_l l JOIN ties_r r ON l.x > r.x AND l.y <= r.y) AS cnt;
SELECT '>= <', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l JOIN ties_r r ON l.x >= r.x AND l.y < r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_l l, ties_r r WHERE l.x >= r.x AND l.y < r.y) AS ok, (SELECT count() FROM ties_l l JOIN ties_r r ON l.x >= r.x AND l.y < r.y) AS cnt;

-- Self-join over the duplicate-heavy keys: with strict conditions a row must never match its
-- own other-side copy.
SELECT 'self', (SELECT (count(), sum(cityHash64(s1.id, s2.id))) FROM ties_l s1 JOIN ties_l s2 ON s1.x < s2.x AND s1.y > s2.y) = (SELECT (count(), sum(cityHash64(s1.id, s2.id))) FROM ties_l s1, ties_l s2 WHERE s1.x < s2.x AND s1.y > s2.y) AS ok, (SELECT count() FROM ties_l s1 JOIN ties_l s2 ON s1.x < s2.x AND s1.y > s2.y) AS cnt;

-- One single tie run: every first-condition key is equal on both sides, so the whole merge is
-- one boundary-crossing tie. A loose first condition accepts all pairs, a strict one none.
CREATE TABLE ties_const_l (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ties_const_r (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ties_const_l SELECT number, 42, number % 11 FROM numbers(700);
INSERT INTO ties_const_r SELECT number, 42, number % 13 FROM numbers(700);

SELECT 'plan const', count() > 0 FROM (EXPLAIN SELECT count() FROM ties_const_l l JOIN ties_const_r r ON l.x <= r.x AND l.y < r.y) WHERE explain LIKE '%IEJoin%';

SELECT 'const <=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_const_l l JOIN ties_const_r r ON l.x <= r.x AND l.y < r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_const_l l, ties_const_r r WHERE l.x <= r.x AND l.y < r.y) AS ok, (SELECT count() FROM ties_const_l l JOIN ties_const_r r ON l.x <= r.x AND l.y < r.y) AS cnt;
SELECT 'const <', (SELECT count() FROM ties_const_l l JOIN ties_const_r r ON l.x < r.x AND l.y < r.y) AS cnt;
SELECT 'const >=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_const_l l JOIN ties_const_r r ON l.x >= r.x AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM ties_const_l l, ties_const_r r WHERE l.x >= r.x AND l.y > r.y) AS ok, (SELECT count() FROM ties_const_l l JOIN ties_const_r r ON l.x >= r.x AND l.y > r.y) AS cnt;
SELECT 'const >', (SELECT count() FROM ties_const_l l JOIN ties_const_r r ON l.x > r.x AND l.y > r.y) AS cnt;

DROP TABLE ties_l;
DROP TABLE ties_r;
DROP TABLE ties_const_l;
DROP TABLE ties_const_r;
