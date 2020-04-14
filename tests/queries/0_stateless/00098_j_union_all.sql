SELECT * FROM (SELECT dummy, -1 as x UNION ALL SELECT dummy, arrayJoin([-1]) as x);
SELECT * FROM (SELECT -1 as x, dummy UNION ALL SELECT arrayJoin([-1]) as x, dummy);
