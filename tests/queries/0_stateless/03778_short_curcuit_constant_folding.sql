select if(x = 1, s, toFixedString(s, 1)) from (select 1 as x, 'aaaa' as s);
WITH test AS (SELECT 0 AS number)
SELECT if(number = 0, 0, intDiv(42, number)) FROM test;

