WITH round(exp(number), 6) AS x, toUInt64(x) AS y, toInt32(x) AS z
SELECT 
	formatReadableTimeDelta(y), 
	formatReadableTimeDelta(z)
FROM system.numbers
LIMIT 30;
