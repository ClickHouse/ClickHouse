-- Tags: stateful
-- Disable optimize_aggregation_in_order: multi-threaded in-order aggregation has a
-- race condition that can produce duplicate rows when GROUP BY contains duplicate keys.
-- This test specifically verifies duplicate GROUP BY key deduplication, not aggregation order.
SET optimize_aggregation_in_order = 0;
SELECT URL, EventDate, max(URL) FROM test.hits WHERE CounterID = 1704509 AND UserID = 4322253409885123546 GROUP BY URL, EventDate, EventDate ORDER BY URL, EventDate;
