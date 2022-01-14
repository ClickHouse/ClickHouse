SELECT x, arrayJoin(x) FROM (SELECT materialize([1, NULL, 2]) AS x);
SELECT x, arrayJoin(x) FROM (SELECT materialize([(1, 2), (3, 4), (5, 6)]) AS x);
SELECT x, arrayJoin(x) FROM (SELECT materialize(arrayMap(x -> toFixedString(x, 5), ['Hello', 'world'])) AS x);
