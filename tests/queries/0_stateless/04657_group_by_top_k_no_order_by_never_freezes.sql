-- The profitability freeze must stay disabled for `GROUP BY keys LIMIT N` without
-- `ORDER BY`.  That shape's plan carries a synthesized sort which only pays off
-- while the heap bounds the hash table: a frozen heap leaves the sort finalizing
-- and ordering every group, where the un-optimized plan would have cancelled
-- after N rows.  A stream whose first rows hold few keys and whose later rows
-- explode is exactly what used to trigger it - the heap fills with nothing to
-- reject, freezes, and then the table grows without bound.
--
-- With a real `ORDER BY` the freeze stays available, because falling back there
-- costs nothing: the sort belongs to the query either way.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET group_by_top_k_optimization_observation_rows = 65536;
SET optimize_trivial_group_by_limit_query = 0;
-- One stream, so the assertions below describe a single heap.
SET max_threads = 1;
-- Single-node heap mechanics: under parallel replicas the no-`ORDER BY` shape
-- is gated off entirely, and memory-bound merging turns the partial aggregation
-- in-order, dropping the heap the assertions below observe.
SET enable_parallel_replicas = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS t_no_order_freeze;

CREATE TABLE t_no_order_freeze (k UInt64) ENGINE = MergeTree ORDER BY tuple();

-- The first 200000 rows hold 5 distinct keys, so the heap fills to its capacity
-- with nothing to reject and outlives the observation window; the next 500000
-- rows are all distinct.  Everything goes into a single part in a single insert
-- block: parts can be read in any order, and the phases must reach the
-- aggregation in this order.
SET max_insert_threads = 1;
SET min_insert_block_size_rows = 1000000;
INSERT INTO t_no_order_freeze
SELECT if(number < 200000, number % 5, 1000000 + number) FROM numbers(700000);

SELECT k, count() FROM t_no_order_freeze GROUP BY k LIMIT 5
SETTINGS log_comment = '04657_no_order_by' FORMAT Null;

SELECT k, count() FROM t_no_order_freeze GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS log_comment = '04657_with_order_by' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Without `ORDER BY`: no freeze, and the heap rejects every one of the 500000
-- exploded keys.  A rejected row never reaches the hash table, so this count is
-- also what bounds it: a key that leaked in would be a row that was not skipped.
-- A lone `count()` state lives inline in the hash-table cell, so this shape
-- allocates no aggregate-state arena at all.
SELECT 'no_order_by: frozen, rejected_all, no arena';
SELECT
    max(ProfileEvents['AggregationTopKHeapsFrozen']),
    max(ProfileEvents['AggregationTopKRowsSkipped']) = 500000,
    max(ProfileEvents['ArenaAllocBytes']) = 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04657_no_order_by';

-- With `ORDER BY` the freeze is still reachable for the same data.
SELECT 'with_order_by: freeze still available';
SELECT max(ProfileEvents['AggregationTopKHeapsFrozen']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04657_with_order_by';

-- Results are unaffected: every group the no-`ORDER BY` query returns carries its
-- complete count (which groups it returns is arbitrary - none of them tie here,
-- but the LIMIT has no ordering to appeal to).
-- `enable_group_by_top_k_optimization` takes effect per query, not per subquery,
-- so the unoptimized reference answer needs its own statement.
DROP TABLE IF EXISTS gt_no_order_freeze;
CREATE TABLE gt_no_order_freeze ENGINE = Memory EMPTY AS
SELECT k, count() AS c FROM t_no_order_freeze GROUP BY k;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_no_order_freeze
SELECT k, count() AS c FROM t_no_order_freeze GROUP BY k;
SET enable_group_by_top_k_optimization = 1;

SELECT 'returned groups complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.c = f.c AS complete
    FROM (SELECT k, count() AS c FROM t_no_order_freeze GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_no_order_freeze AS f USING (k)
);

DROP TABLE gt_no_order_freeze;

DROP TABLE t_no_order_freeze;
