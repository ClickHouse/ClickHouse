-- Tags: no-old-analyzer

-- The position of `ie_join` in the `join_algorithm` list sets its priority over the
-- equality-based algorithms. Listed last, it takes only joins that hash cannot execute
-- (no equality conditions in the ON section; a null-safe equality counts as an equality).
-- Listed first, it takes any join with two inequality conditions: the remaining conditions
-- (including equalities) are applied as a filter over the join result (ALL INNER) or as a
-- residual condition inside the operator (the other kinds), so both routes must produce
-- the same result.

-- Pin the setting (it is randomized in tests): with `ie_join` first the runtime-filter pass
-- must leave the join alone instead of pinning it to a hash-family algorithm.
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS prio_l;
DROP TABLE IF EXISTS prio_r;

CREATE TABLE prio_l (k Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prio_r (k Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO prio_l SELECT number % 5, number, 100 - number FROM numbers(50);
INSERT INTO prio_r SELECT number % 5, number + 3, 90 - number FROM numbers(50);

-- A join with only inequality conditions goes to IEJoin from any position in the list
SET join_algorithm = 'hash,ie_join';
SELECT 'inequalities, ie_join last', count() > 0 FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y;

SET join_algorithm = 'ie_join,hash';
SELECT 'inequalities, ie_join first', count() > 0 FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y;

-- With an equality condition the algorithm listed first takes the join
SET join_algorithm = 'hash,ie_join';
SELECT 'equality, hash first', count() FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y;

-- An equality-bearing outer join also stays on the hash path when hash is listed first
SELECT 'left outer, hash first', count() FROM (EXPLAIN SELECT count() FROM prio_l l LEFT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

-- A null-safe equality counts as an equality condition and keeps the join on the hash path
SELECT 'null-safe equality, hash first', count() FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON (l.k IS NOT DISTINCT FROM r.k) AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

SET join_algorithm = 'ie_join,hash';
SELECT 'equality, ie_join first', count() > 0 FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y;

-- One-sided and constant conditions are also applied as a filter when IEJoin is forced
SELECT 'one-sided, ie_join first', count() > 0 FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y AND l.k > 1 AND r.k < 4) WHERE explain LIKE '%IEJoin%';
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y AND l.k > 1 AND r.k < 4;
SELECT count(), sum(cityHash64(l.k, l.x, l.y, r.k, r.x, r.y)) FROM prio_l l JOIN prio_r r ON l.x < r.x AND l.y > r.y AND l.k > 1 AND r.k < 4 SETTINGS join_algorithm = 'hash';

-- A single inequality is not enough for IEJoin even when it is listed first
SELECT 'single inequality', count() FROM (EXPLAIN SELECT count() FROM prio_l l JOIN prio_r r ON l.k = r.k AND l.x < r.x) WHERE explain LIKE '%IEJoin%';

-- For non-INNER kinds the extra ON conditions affect matching and cannot be applied as a
-- filter over the result: forced IEJoin evaluates them as a residual condition inside the
-- operator. The residual is visible in the plan, and every non-INNER kind must match hash
-- row for row.
SELECT 'left outer with equality', count() > 0 FROM (EXPLAIN SELECT count() FROM prio_l l LEFT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'left residual routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM prio_l l LEFT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%Residual filter%';
SELECT 'left vs hash', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l LEFT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l LEFT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'right vs hash', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l RIGHT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l RIGHT JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'full vs hash', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l FULL JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prio_l l FULL JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'semi vs hash', (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prio_l l LEFT SEMI JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prio_l l LEFT SEMI JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'anti vs hash', (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prio_l l LEFT ANTI JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prio_l l LEFT ANTI JOIN prio_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);

-- IEJoin alone cannot execute a join without inequality conditions
SET join_algorithm = 'ie_join';
SELECT count() FROM prio_l l JOIN prio_r r ON l.k = r.k; -- { serverError NOT_IMPLEMENTED }

DROP TABLE prio_l;
DROP TABLE prio_r;
