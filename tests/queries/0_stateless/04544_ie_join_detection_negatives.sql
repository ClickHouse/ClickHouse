-- Tags: no-old-analyzer

-- ON shapes with fewer than two usable inequality conditions that must NOT be routed through
-- IEJoin, and the behavior of the eligible shape without `ie_join` in `join_algorithm`. For
-- INNER the rejected shapes fall back to a cross join with a filter (no `IEJoin` in the plan,
-- same result); for outer kinds the fallback path cannot determine join keys and the query
-- fails, exactly as without `ie_join` in the list.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS neg_l;
DROP TABLE IF EXISTS neg_r;

CREATE TABLE neg_l (id Int32, x Int32, y Int32, s String, dec Decimal64(2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE neg_r (id Int32, x Int32, y Int32, s String, f Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO neg_l VALUES (1, 1, 3, 'a', 1.5), (2, 2, 2, 'ab', 2.5), (3, 3, 1, 'b', 0.5);
INSERT INTO neg_r VALUES (1, 1, 1, 'a%', 1.0), (2, 2, 3, 'b%', 2.0), (3, 3, 2, '%', 3.0);

-- A one-sided conjunct: not two table-to-table comparisons.
SELECT 'one-sided', count() FROM (EXPLAIN SELECT count() FROM neg_l l JOIN neg_r r ON l.x < r.x AND l.y > 1) WHERE explain LIKE '%IEJoin%';
SELECT 'one-sided result', (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l JOIN neg_r r ON l.x < r.x AND l.y > 1) = (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l, neg_r r WHERE l.x < r.x AND l.y > 1) AS ok;
SELECT count() FROM neg_l l LEFT JOIN neg_r r ON l.x < r.x AND l.y > 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Both operands of a conjunct from the same side.
SELECT 'same-side', count() FROM (EXPLAIN SELECT count() FROM neg_l l JOIN neg_r r ON l.x < l.y AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'same-side result', (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l JOIN neg_r r ON l.x < l.y AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l, neg_r r WHERE l.x < l.y AND l.y > r.y) AS ok;

-- A non-comparison conjunct.
SELECT 'non-comparison', count() FROM (EXPLAIN SELECT count() FROM neg_l l JOIN neg_r r ON l.x < r.x AND l.s LIKE r.s) WHERE explain LIKE '%IEJoin%';
SELECT 'non-comparison result', (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l JOIN neg_r r ON l.x < r.x AND l.s LIKE r.s) = (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l, neg_r r WHERE l.x < r.x AND l.s LIKE r.s) AS ok;

-- Keys without a common supertype (Decimal vs Float): the filter compares them fine, so the
-- shape must fall back to the cross join instead of failing on the common-type cast.
SELECT 'no-supertype', count() FROM (EXPLAIN SELECT count() FROM neg_l l JOIN neg_r r ON l.dec < r.f AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'no-supertype result', (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l JOIN neg_r r ON l.dec < r.f AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM neg_l l, neg_r r WHERE l.dec < r.f AND l.y > r.y) AS ok;
SELECT count() FROM neg_l l LEFT JOIN neg_r r ON l.dec < r.f AND l.y > r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- An input with totals is routed to IEJoin (the shape is eligible) but the operator's
-- pipeline does not support totals: pin the clean error.
SELECT 'totals routed', count() > 0 FROM (EXPLAIN SELECT count() FROM (SELECT x, max(y) AS c FROM neg_l GROUP BY x WITH TOTALS) l JOIN neg_r r ON l.x < r.x AND l.c > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count()
FROM (SELECT x, max(y) AS c FROM neg_l GROUP BY x WITH TOTALS) l
JOIN neg_r r ON l.x < r.x AND l.c > r.y; -- { serverError NOT_IMPLEMENTED }

-- Without `ie_join` in the list the eligible shape keeps the pre-IEJoin behavior: a cross join
-- with a filter for INNER, an error for outer kinds.
SET join_algorithm = 'direct,parallel_hash,hash';
SELECT 'setting off', count() FROM (EXPLAIN SELECT count() FROM neg_l l JOIN neg_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM neg_l l LEFT JOIN neg_r r ON l.x < r.x AND l.y > r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM neg_l l LEFT ANTI JOIN neg_r r ON l.x < r.x AND l.y > r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE neg_l;
DROP TABLE neg_r;
