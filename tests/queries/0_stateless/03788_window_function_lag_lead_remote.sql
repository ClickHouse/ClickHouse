
-- WindowStep does not support query plan serialization yet
SET serialize_query_plan = 0;

SELECT lag(number) OVER (ORDER BY number)
FROM remote('127.0.0.1', numbers(5));

SELECT lead(number) OVER (ORDER BY number)
FROM remote('127.0.0.1', numbers(5));
