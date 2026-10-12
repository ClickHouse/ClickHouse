-- Tags: no-old-analyzer

-- Strictnesses that must never route to IEJoin even with `ie_join` listed first: ANY with
-- only inequality conditions keeps the pre-existing error, ASOF stays on the ASOF path.

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
