-- Tags: stateful
SELECT PP.Key1 AS `ym:s:paramsLevel1`, sum(arrayAll(`x_1` -> `x_1`= '', ParsedParams.Key2)) AS `ym:s:visits` FROM test.hits ARRAY JOIN ParsedParams AS `PP`  WHERE CounterID = 1704509 GROUP BY `ym:s:paramsLevel1` ORDER BY PP.Key1, `ym:s:visits` LIMIT 0, 100;
SELECT PP.Key1 AS x1, ParsedParams.Key2 AS x2 FROM test.hits ARRAY JOIN ParsedParams AS PP WHERE CounterID = 1704509 ORDER BY x1, x2 LIMIT 10;
SELECT ParsedParams.Key2 AS x FROM test.hits ARRAY JOIN ParsedParams AS PP ORDER BY x DESC LIMIT 10;
