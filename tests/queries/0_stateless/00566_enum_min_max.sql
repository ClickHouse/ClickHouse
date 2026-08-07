SELECT min(x), max(x), sum(x) FROM (SELECT CAST(arrayJoin([1, 2]) AS Enum8('Hello' = 1, 'World' = 2)) AS x);
