-- Tags: no-old-analyzer

-- Shapes that must never route to IEJoin even with `ie_join` listed first: unsupported
-- strictness (ANY, ASOF) and keys IEJoin cannot order (Dynamic). The pre-existing behavior
-- (error or fallback) must be preserved.

SET join_algorithm = 'ie_join,hash';

DROP TABLE IF EXISTS neg_l;
DROP TABLE IF EXISTS neg_r;

CREATE TABLE neg_l (k Int32, t Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE neg_r (k Int32, t Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO neg_l VALUES (1, 10), (1, 20);
INSERT INTO neg_r VALUES (1, 5, 100), (1, 15, 200);

-- ANY strictness with only inequality conditions keeps the pre-existing error
SELECT * FROM neg_l l ANY LEFT JOIN neg_r r ON l.k < r.v AND l.t > r.t; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- ASOF join stays on the ASOF path (its ON has an equality plus one inequality)
SELECT 'asof not routed', count() FROM (EXPLAIN SELECT * FROM neg_l l ASOF JOIN neg_r r ON l.k = r.k AND l.t >= r.t) WHERE explain LIKE '%IEJoin%';
SELECT 'asof', l.t, r.v FROM neg_l l ASOF JOIN neg_r r ON l.k = r.k AND l.t >= r.t ORDER BY ALL;

DROP TABLE neg_l;
DROP TABLE neg_r;

-- Dynamic keys have no total order for IEJoin: INNER falls back to a cross join with
-- a filter, the outer kinds keep the pre-existing error
DROP TABLE IF EXISTS dyn_l;
DROP TABLE IF EXISTS dyn_r;

CREATE TABLE dyn_l (x Dynamic, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dyn_r (a Dynamic, b Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dyn_l VALUES (1, 1), (5, 2);
INSERT INTO dyn_r VALUES (3, 0), (7, 5);

SELECT 'dynamic not routed', count() FROM (EXPLAIN SELECT * FROM dyn_l l JOIN dyn_r r ON l.x < r.a AND l.y > r.b) WHERE explain LIKE '%IEJoin%';
SELECT 'dynamic inner', count() FROM dyn_l l JOIN dyn_r r ON l.x < r.a AND l.y > r.b;
SELECT count() FROM dyn_l l LEFT JOIN dyn_r r ON l.x < r.a AND l.y > r.b; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE dyn_l;
DROP TABLE dyn_r;
