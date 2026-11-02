-- Tags: no-old-analyzer
-- `ie_join` is not in the default `join_algorithm`, so both queries pin the algorithm themselves.

DROP TABLE IF EXISTS ie_l;
DROP TABLE IF EXISTS ie_r;

CREATE TABLE ie_l (id Int32, lo Int32, hi Int32, price Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ie_r (id Int32, lo Int32, hi Int32, bid Float64) ENGINE = MergeTree ORDER BY id;

INSERT INTO ie_l VALUES (1, 10, 20, 150.0), (2, 20, 30, 151.0), (3, 30, 40, 380.0);
INSERT INTO ie_r VALUES (1, 10, 30, 149.5), (2, 30, 40, 150.5), (3, 40, 50, 379.0);

-- A cross-side `arrayJoin` lands in the condition that `ie_join` evaluates during the join.
SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A one-sided `arrayJoin` is extracted into a filter before the join, so it stays accepted.
SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (l.price > arrayJoin(range(3))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1,
        query_plan_split_filter = 1;

DROP TABLE ie_l;
DROP TABLE ie_r;
