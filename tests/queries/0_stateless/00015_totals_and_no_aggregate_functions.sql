-- Tags: stateful
SELECT AdvEngineID FROM test.hits GROUP BY AdvEngineID WITH TOTALS ORDER BY AdvEngineID
