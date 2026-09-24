-- A higher-order function whose lambda tests membership of an IN subquery, over a table read from a
-- remote shard, with a LIMIT range: the query is rejected because such a lambda cannot be sent to the
-- shard, but rejecting it must not take the server down with it.

-- The read really goes through a remote source, so the shard header is converted on the way back.
SELECT max(explain LIKE '%Remote%') FROM (EXPLAIN PIPELINE graph = 1 SELECT arrayExists(x -> (x IN (SELECT 2)), [2]) FROM remote('127.0.0.{2,3}', system.one) LIMIT UNTIL 1);

SELECT arrayExists(x -> (x IN (SELECT 2)), [2]) FROM remote('127.0.0.{2,3}', system.one) LIMIT UNTIL 1; -- { serverError NOT_IMPLEMENTED }
