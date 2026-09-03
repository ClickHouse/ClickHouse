select toUInt64(1) union all select countIf(n>0) from (select 2 as n);
SELECT q FROM (select [1,2,3] AS q UNION ALL select groupArray(arrayJoin([4,5,6])) AS q ORDER BY q) ORDER BY q;
