#!/usr/bin/env -S ${HOME}/clickhouse-client --progress --queries-file

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6);

-- SET analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 1 as k FROM (SELECT 10 AS a) t1 JOIN (SELECT 1 AS k, 20 AS b) t2 USING k;

SELECT count(1) FROM (
    SELECT materialize(1) as k, n, m FROM (SELECT number m FROM numbers(10) ) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SELECT count(1) FROM (
    SELECT materialize(1) as k, m FROM (SELECT number m FROM numbers(10) ) nums
    JOIN (SELECT materialize(1) AS k, number m FROM numbers(10)) j
    USING k, m);

SELECT count(1) FROM (
    SELECT materialize(1) as k, m, 2 as n FROM (SELECT number m FROM numbers(10) ) nums
    JOIN (SELECT materialize(1) AS k, number m, 2 as n FROM numbers(10)) j
    USING k, m, n);

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM t1
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM t1 as ttt
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);
