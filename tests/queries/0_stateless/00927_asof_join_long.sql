-- Tags: long, no-asan, no-msan, no-tsan

DROP TABLE IF EXISTS tvs;

-- to use different algorithms for in subquery
SET enable_analyzer = 1;

CREATE TABLE tvs(k UInt32, t UInt32, tv UInt64) ENGINE = Memory;
INSERT INTO tvs(k,t,tv) SELECT k, t, t
FROM (SELECT toUInt32(number) AS k FROM numbers(1000)) keys
CROSS JOIN (SELECT toUInt32(number * 3) as t FROM numbers(10000)) tv_times
SETTINGS join_algorithm = 'hash';

SELECT SUM(trades.price - tvs.tv) FROM
(SELECT k, t, t as price
    FROM (SELECT toUInt32(number) AS k FROM numbers(1000)) keys
    CROSS JOIN (SELECT toUInt32(number * 10) AS t FROM numbers(3000)) trade_times
    SETTINGS join_algorithm = 'hash') trades
ASOF LEFT JOIN tvs USING(k,t);

SELECT SUM(trades.price - tvs.tv) FROM
(SELECT k, t, t as price
    FROM (SELECT toUInt32(number) AS k FROM numbers(1000)) keys
    CROSS JOIN (SELECT toUInt32(number * 10) AS t FROM numbers(3000)) trade_times
    SETTINGS join_algorithm = 'hash') trades
ASOF LEFT JOIN tvs USING(k,t)
SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE tvs;
