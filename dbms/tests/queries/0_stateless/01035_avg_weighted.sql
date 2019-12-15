SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 5), (2, 4), (3, 3), (4, 2), (5, 1)]) AS t));
SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]) AS t));

SELECT avgWeighted(toDecimal64(0, 0), toFloat64(0))