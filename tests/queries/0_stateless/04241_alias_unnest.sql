-- unnest is a PostgreSQL alias of arrayJoin (single-argument).
SELECT unnest([1, 2, 3]);
SELECT UNNEST(['a', 'b']);
SELECT sum(x) FROM (SELECT unnest([10, 20, 30]) AS x);
