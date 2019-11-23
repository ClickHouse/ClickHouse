SELECT avgWeighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]) AS t));
