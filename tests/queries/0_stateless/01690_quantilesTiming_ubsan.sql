-- NOTE: that due to overflows it may give different result before
-- quantilesTimingWeighted() had been converted to double:
--
-- Before:
--
--     SELECT quantilesTimingWeighted(1)(number, 9223372036854775807)
--     FROM numbers(2)
--
--     ┌─quantilesTimingWeighted(1)(number, 9223372036854775807)─┐
--     │ [1]                                                     │
--     └─────────────────────────────────────────────────────────┘
--
-- After:
--
--     SELECT quantilesTimingWeighted(1)(number, 9223372036854775807)
--     FROM numbers(2)
--
--     ┌─quantilesTimingWeighted(1)(number, 9223372036854775807)─┐
--     │ [0]                                                     │
--     └─────────────────────────────────────────────────────────┘

SELECT quantilesTimingWeighted(0.1)(number, 9223372036854775807) FROM numbers(2);

-- same UB, but in the inner loop
SELECT quantilesTimingWeighted(0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1)(number, 9223372036854775807)
FROM
(
    SELECT number
    FROM system.numbers
    LIMIT 100
);
