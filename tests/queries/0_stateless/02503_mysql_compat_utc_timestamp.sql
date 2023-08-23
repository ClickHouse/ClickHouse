-- PowerBI is doing this query. It should work at least somehow, not necessarily in the same way as in MySQL.
SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) DIV 600;
