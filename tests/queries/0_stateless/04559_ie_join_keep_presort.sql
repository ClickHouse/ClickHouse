-- Tags: no-old-analyzer

-- The pre-join sorts IEJoin relies on are not redundant: `query_plan_remove_redundant_sorting`
-- must keep both `Sort ... before JOIN` steps under an outer ORDER BY, and the result must
-- stay correct.

SET join_algorithm = 'ie_join,hash';
SET query_plan_remove_redundant_sorting = 1;

DROP TABLE IF EXISTS ps_l;
DROP TABLE IF EXISTS ps_r;

CREATE TABLE ps_l (id UInt32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ps_r (id UInt32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ps_l SELECT number + 1, toInt32(number * 7 % 41), toInt32(100 - number * 5 % 37) FROM numbers(200);
INSERT INTO ps_r SELECT number + 1, toInt32(number * 11 % 43 + 1), toInt32(90 - number * 3 % 31) FROM numbers(200);

SELECT 'pre-join sorts kept', countIf(explain LIKE '%Sort % before JOIN%') FROM (
    EXPLAIN SELECT l.id, r.id FROM ps_l l JOIN ps_r r ON l.x < r.x AND l.y > r.y ORDER BY l.id, r.id
);

SELECT 'ordered result', (
    SELECT groupArray((l_id, r_id)) FROM (SELECT l.id AS l_id, r.id AS r_id FROM ps_l l JOIN ps_r r ON l.x < r.x AND l.y > r.y ORDER BY l.id, r.id)
) = (
    SELECT groupArray((l_id, r_id)) FROM (SELECT l.id AS l_id, r.id AS r_id FROM ps_l l JOIN ps_r r ON l.x < r.x AND l.y > r.y ORDER BY l.id, r.id
        SETTINGS join_algorithm = 'hash', query_plan_remove_redundant_sorting = 0)
) AS ok, (SELECT count() FROM ps_l l JOIN ps_r r ON l.x < r.x AND l.y > r.y) AS cnt;

DROP TABLE ps_l;
DROP TABLE ps_r;
