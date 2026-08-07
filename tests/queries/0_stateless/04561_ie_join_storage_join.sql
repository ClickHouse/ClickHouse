-- Tags: no-old-analyzer

-- A `Join` engine table on the right side keeps the storage-join path even when `ie_join`
-- is listed first in `join_algorithm`.

DROP TABLE IF EXISTS ie_sj_left;
DROP TABLE IF EXISTS ie_sj_inner;
DROP TABLE IF EXISTS ie_sj_left_eng;
SET join_algorithm = 'ie_join,hash';

DROP TABLE IF EXISTS ie_sj_left;
DROP TABLE IF EXISTS ie_sj_inner;
DROP TABLE IF EXISTS ie_sj_left_eng;

CREATE TABLE ie_sj_left (k Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ie_sj_inner (k Int32, x Int32, y Int32) ENGINE = Join(ALL, INNER, k);
CREATE TABLE ie_sj_left_eng (k Int32, x Int32, y Int32) ENGINE = Join(ALL, LEFT, k);

INSERT INTO ie_sj_left VALUES (1, 1, 10), (2, 5, 20), (3, 7, 30), (4, 2, 40);
INSERT INTO ie_sj_inner VALUES (1, 3, 5), (2, 9, 1), (3, 1, 40);
INSERT INTO ie_sj_left_eng VALUES (1, 3, 5), (2, 9, 1), (3, 1, 40);

SELECT 'inner not routed', count() FROM (
    EXPLAIN SELECT l.k FROM ie_sj_left l JOIN ie_sj_inner r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) WHERE explain LIKE '%IEJoin%';

SELECT 'inner', l.k, l.x, l.y, r.x, r.y
FROM ie_sj_left l JOIN ie_sj_inner r ON l.k = r.k AND l.x < r.x AND l.y > r.y
ORDER BY ALL;

-- An outer join whose ON section the `Join` engine cannot evaluate keeps its pre-existing error
SELECT count() FROM ie_sj_left l LEFT JOIN ie_sj_left_eng r ON l.k = r.k AND l.x < r.x AND l.y > r.y; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

-- Without an equality on the storage key the storage join keeps its pre-existing error too
SELECT count() FROM ie_sj_left l JOIN ie_sj_inner r ON l.x < r.x AND l.y > r.y; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

DROP TABLE ie_sj_left;
DROP TABLE ie_sj_inner;
DROP TABLE ie_sj_left_eng;
