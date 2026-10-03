-- Tags: shard

-- A projection alias that reuses the name of a column expanded from `*` gives a result with two
-- columns of the same name. Each of them must keep its own values when a shard runs the query.

DROP TABLE IF EXISTS t_05320;
CREATE TABLE t_05320 (d Date, v UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_05320 VALUES ('2026-01-01', 1), ('2026-01-02', 2);

SELECT *, d + 365 AS d FROM remote('127.0.0.2', currentDatabase(), t_05320) ORDER BY v FORMAT TSVWithNames;
SELECT *, number + 10 AS number FROM remote('127.0.0.2', numbers(2)) ORDER BY number FORMAT TSVWithNames;
SELECT *, toUInt64(5) AS number FROM remote('127.0.0.2', numbers(1));

-- Local shard.
SELECT *, d + 365 AS d FROM remote('127.0.0.1', currentDatabase(), t_05320) ORDER BY v SETTINGS prefer_localhost_replica = 1;
SELECT *, d + 365 AS d FROM remote('127.0.0.1', currentDatabase(), t_05320) ORDER BY v SETTINGS prefer_localhost_replica = 0;

SELECT *, d + 365 AS d FROM remote('127.0.0.2', currentDatabase(), t_05320) ORDER BY v SETTINGS serialize_query_plan = 1;

-- Two shards, each running the query to completion.
SELECT *, d + 365 AS d FROM remote('127.0.0.{2,3}', currentDatabase(), t_05320) WHERE v = 1 SETTINGS distributed_group_by_no_merge = 1;

SELECT *, number + 10 AS number FROM remote('127.0.0.2', numbers(2)) ORDER BY number SETTINGS extremes = 1;
SELECT *, sum(v) AS v FROM remote('127.0.0.2', currentDatabase(), t_05320) GROUP BY ALL WITH TOTALS ORDER BY d;

-- Same expression twice, and a named table expression.
SELECT v, v FROM remote('127.0.0.2', currentDatabase(), t_05320) ORDER BY 1;
SELECT *, d + 365 AS d FROM remote('127.0.0.2', currentDatabase(), t_05320) AS r ORDER BY v;

DROP TABLE t_05320;
