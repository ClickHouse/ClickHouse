SET max_rows_to_read = 1000;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 != 0 GROUP BY CounterID;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 AND CounterID = 34 GROUP BY CounterID;
