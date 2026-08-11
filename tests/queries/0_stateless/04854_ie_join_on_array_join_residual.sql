-- Tags: no-old-analyzer
-- `ie_join` is not in the default `join_algorithm`, and no randomization list contains it,
-- so every query below pins the algorithm itself.

DROP TABLE IF EXISTS ie_l;
DROP TABLE IF EXISTS ie_r;

CREATE TABLE ie_l (id Int32, lo Int32, hi Int32, price Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ie_r (id Int32, lo Int32, hi Int32, bid Float64) ENGINE = MergeTree ORDER BY id;

INSERT INTO ie_l VALUES (1, 10, 20, 150.0), (2, 20, 30, 151.0), (3, 30, 40, 380.0);
INSERT INTO ie_r VALUES (1, 10, 30, 149.5), (2, 30, 40, 150.5), (3, 40, 50, 379.0);

SELECT '--- rejected: cross-side arrayJoin in a residual, every supported kind';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l SEMI RIGHT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l LEFT ANTI JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l RIGHT ANTI JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l ALL LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l ALL RIGHT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT l.id FROM ie_l AS l ALL FULL JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: a large expansion read out of bounds instead of past the padding';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(1000)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: the unnest alias resolves to arrayJoin and is refused too';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, unnest(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: not dependent on join_use_nulls';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- rejected: a one-sided arrayJoin that is not split out of the ON clause';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (l.price > arrayJoin(range(3))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1,
        query_plan_split_filter = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT '--- kept: a one-sided arrayJoin is extracted into a filter before the join';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (l.price > arrayJoin(range(3))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1,
        query_plan_split_filter = 1;

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (r.bid > arrayJoin(range(3))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1,
        query_plan_split_filter = 1;

SELECT '--- kept: ALL INNER applies the condition after the join instead of building a residual';

SELECT l.id FROM ie_l AS l ALL INNER JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(3)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT '--- kept: a residual without arrayJoin still runs';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > l.price + r.bid) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT l.id FROM ie_l AS l ALL FULL JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > l.price + r.bid) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT l.id FROM ie_l AS l LEFT ANTI JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > l.price + r.bid) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT '--- kept: an equality key over arrayJoin is extracted before the join';

SELECT l.id FROM ie_l AS l ALL INNER JOIN ie_r AS r ON l.lo = arrayJoin([r.lo, r.hi]) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT '--- kept: the supported rewrite, ARRAY JOIN in a subquery before the join';

SELECT l.id FROM (SELECT id, lo, hi, price, k FROM ie_l ARRAY JOIN range(2) AS k) AS l
    SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi) AND (300 > l.price + r.bid - l.k)
    ORDER BY ALL SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1;

SELECT '--- kept: without the analyzer the condition is refused during analysis';

SELECT l.id FROM ie_l AS l SEMI LEFT JOIN ie_r AS r ON (l.lo < r.hi) AND (r.lo < l.hi)
    AND (300 > minus(l.price + r.bid, arrayJoin(range(2)))) ORDER BY ALL
    SETTINGS join_algorithm = 'ie_join,hash', join_use_nulls = 1,
        enable_analyzer = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE ie_l;
DROP TABLE ie_r;
