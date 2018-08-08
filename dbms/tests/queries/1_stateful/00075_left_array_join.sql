SELECT UserID, EventTime, pp.Key1, pp.Key2, ParsedParams.Key1 FROM test.hits ARRAY JOIN ParsedParams AS pp WHERE CounterID = 1143050 ORDER BY UserID, EventTime, pp.Key1, pp.Key2 LIMIT 100;
SELECT UserID, EventTime, pp.Key1, pp.Key2, ParsedParams.Key1 FROM test.hits LEFT ARRAY JOIN ParsedParams AS pp WHERE CounterID = 1143050 ORDER BY UserID, EventTime, pp.Key1, pp.Key2 LIMIT 100;
