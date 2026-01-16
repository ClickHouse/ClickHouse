SELECT topKWeighted(2)(x, weight), topK(2)(x) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([('hello', 1), ('world', 1), ('goodbye', 1), ('abc', 1)]) AS t));
SELECT topKWeighted(2)(x, weight), topK(2)(x) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT arrayJoin([('hello', 1), ('world', 1), ('goodbye', 2), ('abc', 1)]) AS t));
SELECT topKWeighted(5)(n, weight) FROM (SELECT number as n, number as weight from system.numbers LIMIT 100);
