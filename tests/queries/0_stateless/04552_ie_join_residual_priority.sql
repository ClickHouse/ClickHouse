-- Tags: no-old-analyzer

-- `ie_join` listed first in `join_algorithm` claims outer/semi/anti joins whose ON has two
-- inequality conditions plus equality conditions: the equalities become the operator's
-- residual condition and the result must match the hash join executing the same query.
-- Listed after hash, joins with equality conditions stay on the hash path.

-- Pin the setting (it is randomized in tests): with `ie_join` first the runtime-filter pass
-- must leave the join alone instead of pinning it to a hash-family algorithm.
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS prl;
DROP TABLE IF EXISTS prr;

CREATE TABLE prl (k Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prr (k Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO prl SELECT number % 7, number, 100 - number FROM numbers(60);
INSERT INTO prr SELECT number % 7, number + 3, 95 - number FROM numbers(60);

SET join_algorithm = 'ie_join,hash';

SELECT 'left routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM prl l LEFT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%Residual filter%';
SELECT 'left', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l LEFT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l LEFT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'right', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l RIGHT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l RIGHT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'full', (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l FULL JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y, r.k, r.x, r.y))) FROM prl l FULL JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'semi', (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prl l LEFT SEMI JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prl l LEFT SEMI JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);
SELECT 'anti', (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prl l LEFT ANTI JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
) = (
    SELECT arraySort(groupArray((l.k, l.x, l.y))) FROM prl l LEFT ANTI JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'hash'
);

-- Listed after hash, the equality-bearing outer join keeps the hash path
SET join_algorithm = 'hash,ie_join';
SELECT 'hash first', count() FROM (EXPLAIN SELECT count() FROM prl l LEFT JOIN prr r ON l.k = r.k AND l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

DROP TABLE prl;
DROP TABLE prr;
