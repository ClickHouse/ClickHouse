-- Tags: stateful
SELECT SearchEngineID AS k1, AdvEngineID AS k2, count() AS c FROM test.hits GROUP BY k1, k2 ORDER BY c DESC, k1, k2 LIMIT 10
