-- Tags: distributed

-- Distributed `DISTINCT` with `LIMIT` returns ten unique rows without requiring an order.
SELECT count(), uniqExact(number)
FROM
(
    SELECT DISTINCT number FROM remote('127.0.0.{2,3}', system.numbers) LIMIT 10
);
