-- Tags: stateful
SELECT count() FROM test.hits PREWHERE UserID IN (SELECT UserID FROM test.hits WHERE CounterID = 800784);
