-- A zero key-count threshold freezes each producer before its first key is inserted. The empty
-- frozen tables contribute no measured per-key cost, and all input is staged for the merge.
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 0;
SET max_threads = 4;
SET max_block_size = 8192;
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 5000000;

SELECT 'Immediate freeze with inline count';
SELECT count(), sum(c), sum(k)
FROM
(
    SELECT number AS k, count() AS c
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'Immediate freeze with sum';
SELECT count(), sum(sm)
FROM
(
    SELECT number % 10000 AS k, sum(number) AS sm
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'Immediate freeze with growing distinct states';
SELECT count(), sum(u)
FROM
(
    SELECT number % 10000 AS k, uniqExact(number) AS u
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'Immediate freeze without aggregate states';
SELECT count(), sum(k)
FROM
(
    SELECT number AS k
    FROM numbers_mt(1000000)
    GROUP BY k
);

SELECT 'Immediate freeze with fixed distinct states';
SELECT
    (SELECT sum(u) FROM
        (SELECT number % 10000 AS k, uniqHLL12(number) AS u
         FROM numbers_mt(1000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(u) FROM
        (SELECT number % 10000 AS k, uniqHLL12(number) AS u
         FROM numbers_mt(1000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

-- A positive threshold retains keys and measures their allocation cost before freezing.
SET adaptive_aggregator_freeze_threshold = 1;
SELECT 'Freeze with retained keys';
SELECT count(), sum(sm)
FROM
(
    SELECT number AS k, sum(number) AS sm
    FROM numbers_mt(1000000)
    GROUP BY k
);
