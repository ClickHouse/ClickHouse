-- When a part of a WHERE condition is pushed down and the rest of it stays in the result, the
-- remaining conjunct must keep the semantics of `and`: every non-zero value is true, not only the
-- ones that survive a cast to UInt8.

DROP TABLE IF EXISTS t_push_down_filter;

CREATE TABLE t_push_down_filter (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_push_down_filter SELECT number FROM numbers(600);

SELECT count(), sum(f) FROM
(
    SELECT id, (id != 1000 AND s) AS f FROM (SELECT id, sum(id) AS s FROM t_push_down_filter GROUP BY id) WHERE id != 1000 AND s
)
SETTINGS query_plan_filter_push_down = 1;

SELECT count(), sum(f) FROM
(
    SELECT id, (id != 1000 AND s) AS f FROM (SELECT id, sum(id) AS s FROM t_push_down_filter GROUP BY id) WHERE id != 1000 AND s
)
SETTINGS query_plan_filter_push_down = 0;

SELECT id, f FROM
(
    SELECT id, (id != 1000 AND s) AS f FROM (SELECT id, sum(id) AS s FROM t_push_down_filter GROUP BY id) WHERE id != 1000 AND s
)
WHERE id IN (0, 255, 256, 512)
ORDER BY id;

DROP TABLE t_push_down_filter;
