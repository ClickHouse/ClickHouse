-- `parallel_hash` does not keep the order of the left table for a narrow join key, so read-in-order
-- must not be propagated through it. Otherwise aggregation-in-order consumes a stream it wrongly
-- believes is sorted by the GROUP BY key and mis-groups the rows. Issue #109216.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt32, j UInt8) ENGINE = MergeTree ORDER BY (a, j);
CREATE TABLE r (j UInt8) ENGINE = MergeTree ORDER BY j;
INSERT INTO l SELECT intDiv(number, 8)::UInt32, (number % 8)::UInt8 FROM numbers(64);
INSERT INTO r SELECT number::UInt8 FROM numbers(8);

-- 8 groups of 8 rows each. The test runner randomizes the settings below, and the ordered read only
-- happens with read-in-order enabled, the join sides unswapped and the join not spilling.
SELECT a, count() FROM l LEFT JOIN r ON l.j = r.j GROUP BY a ORDER BY a
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8, optimize_aggregation_in_order = 1,
         optimize_read_in_order = 1, query_plan_read_in_order = 1,
         query_plan_read_in_order_through_join = 1, query_plan_join_swap_table = 'false',
         max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
         enable_parallel_replicas = 0;

DROP TABLE l;
DROP TABLE r;
