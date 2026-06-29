-- Tags: no-fasttest
-- no-fasttest: quantileReq requires the DataSketches library, which is disabled in the fast test build.

-- The Relative Error Quantiles (REQ) sketch: a fully-mergeable streaming quantile with a relative
-- rank-error guarantee, accurate at extreme quantiles. See QuantileReq.h.
-- Reference: Cormode, Karnin, Liberty, Thaler, Veselý, "Relative Error Streaming Quantiles"
-- (JACM 2023, https://dl.acm.org/doi/10.1145/3617891).

-- 1. Median sanity on a uniform stream 0..999.
SELECT quantileReq(100, 0.5)(number) BETWEEN 470 AND 530 FROM numbers(1000);

-- 2. Accuracy across several levels on a uniform stream 0..999999.
--    For this distribution the true quantile at level p is p * 999999. REQ must land within a
--    small relative error of the truth at each level.
SELECT
    abs(quantileReq(1000, 0.25)(number)  - 0.25 * 999999) / 999999 < 0.01,
    abs(quantileReq(1000, 0.50)(number)  - 0.50 * 999999) / 999999 < 0.01,
    abs(quantileReq(1000, 0.75)(number)  - 0.75 * 999999) / 999999 < 0.01,
    abs(quantileReq(1000, 0.90)(number)  - 0.90 * 999999) / 999999 < 0.01
FROM numbers(1000000);

-- 3. Extreme-tail accuracy — the regime REQ exists for. p99.9 and p99.99 of 0..999999.
SELECT
    abs(quantileReq(1000, 0.999)(number)  - 0.999  * 999999) / 999999 < 0.01,
    abs(quantileReq(1000, 0.9999)(number) - 0.9999 * 999999) / 999999 < 0.01
FROM numbers(1000000);

-- 4. Mergeability: the same tail quantile computed by merging many partial states (one per
--    partition) must still honour the relative-error guarantee. Exercises serialize/deserialize
--    + merge, i.e. the distributed-aggregation path.
SELECT abs(quantileReqMerge(1000, 0.999)(s) - 0.999 * 999999) / 999999 < 0.01
FROM
(
    SELECT quantileReqState(1000, 0.999)(number) AS s
    FROM numbers(1000000)
    GROUP BY number % 13
);

-- 5. Multiple levels in one call are returned in level order and are monotonically non-decreasing.
SELECT arraySort(x -> x, q) = q
FROM (SELECT quantilesReq(1000, 0.1, 0.5, 0.9, 0.99, 0.999)(number) AS q FROM numbers(1000000));

-- 6. Edge cases — empty input yields NaN (not a logical error), a single value is returned exactly.
SELECT isNaN(quantileReq(12, 0.5)(number)) FROM numbers(0);
SELECT quantileReq(12, 0.5)(number) = 5 FROM numbers(5, 1);

-- 7. Type coverage: DateTime arguments return a DateTime quantile.
--    Median of [start .. start+999] seconds is ~ start + 500s = 00:08:20; allow a generous window.
SELECT quantileReq(100, 0.5)(toDateTime('2020-01-01 00:00:00', 'UTC') + number)
    BETWEEN toDateTime('2020-01-01 00:07:00', 'UTC') AND toDateTime('2020-01-01 00:09:40', 'UTC')
FROM numbers(1000);

-- 8. The 'median' alias is registered.
SELECT medianReq(100, 0.5)(number) BETWEEN 470 AND 530 FROM numbers(1000);

-- 9. Small inputs: when the data fits within the sketch's accuracy budget no compaction happens,
--    so REQ returns exact order statistics. Median of 0..10 is 5, of 0..100 is 50.
SELECT quantileReq(1000, 0.5)(number) = 5 FROM numbers(11);
SELECT quantileReq(1000, 0.5)(number) = 50 FROM numbers(101);

-- 10. Asymmetric merge (1 row vs 999999 rows) — the merged tail quantile still honours the bound.
SELECT abs(quantileReqMerge(1000, 0.999)(s) - 0.999 * 999999) / 999999 < 0.01
FROM
(
    SELECT quantileReqState(1000, 0.999)(number) AS s FROM numbers(1)
    UNION ALL
    SELECT quantileReqState(1000, 0.999)(number) AS s FROM numbers(1, 999999)
);

-- 11. Three-way merge of disjoint partitions covering 0..999999 — still honours the bound.
SELECT abs(quantileReqMerge(1000, 0.999)(s) - 0.999 * 999999) / 999999 < 0.01
FROM
(
    SELECT quantileReqState(1000, 0.999)(number) AS s FROM numbers(400000)
    UNION ALL
    SELECT quantileReqState(1000, 0.999)(number) AS s FROM numbers(400000, 400000)
    UNION ALL
    SELECT quantileReqState(1000, 0.999)(number) AS s FROM numbers(800000, 200000)
);

-- 12. The accuracy parameter is mandatory.
SELECT quantileReq(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
