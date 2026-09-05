-- A UNION ALL whose range branches use WITH TOTALS configures each LimitRangeTransform to drain its
-- input so totals are computed over all rows. The union-level settings limit cap must keep draining
-- (always_read_till_end) so it does not close the pipeline before totals are produced.
-- Each branch produces a single group (g = 0) so the row that survives `limit = 1` is deterministic.

-- { echo }

(SELECT 0 AS g, count() AS c FROM numbers(6) GROUP BY g WITH TOTALS LIMIT AFTER g >= 0) UNION ALL (SELECT 0 AS g, count() AS c FROM numbers(6) GROUP BY g WITH TOTALS LIMIT AFTER g >= 0) SETTINGS limit = 1, max_threads = 1;
(SELECT 0 AS g, count() AS c FROM numbers(6) GROUP BY g WITH TOTALS LIMIT AFTER g >= 0) UNION ALL (SELECT 0 AS g, count() AS c FROM numbers(6) GROUP BY g WITH TOTALS LIMIT AFTER g >= 0) SETTINGS limit = 1, max_threads = 1, enable_analyzer = 0;
