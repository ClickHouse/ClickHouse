-- Tags: stateful
SELECT GoalsReached AS k, count() AS c FROM test.hits GROUP BY k ORDER BY c DESC LIMIT 10;
SELECT GeneralInterests AS k1, GoalsReached AS k2, count() AS c FROM test.hits GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
SELECT ParsedParams.Key1 AS k1, GeneralInterests AS k2, count() AS c FROM test.hits GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
SELECT ParsedParams.Key1 AS k1, GeneralInterests AS k2, count() AS c FROM test.hits WHERE notEmpty(k1) AND notEmpty(k2) GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
