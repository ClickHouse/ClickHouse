SET extremes = 1;
SELECT 'Hello, world' FROM (SELECT number FROM system.numbers LIMIT 10) WHERE number < 0
FORMAT JSONCompact;
