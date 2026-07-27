SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(reinterpret(randomString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);
