SELECT toUInt64(1) x FROM (select 1)
GROUP BY 1
HAVING x
IN ( SELECT countIf(y, z == 1) FROM (SELECT 1 y, 1 z) );
