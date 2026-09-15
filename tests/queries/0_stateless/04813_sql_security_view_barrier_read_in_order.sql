-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for the read-in-order
-- analysis too: an outer `ORDER BY` / `GROUP BY` / `DISTINCT` must not shape how the source
-- below the view's filtering reads, otherwise the read pattern (and so `read_rows` / timing)
-- depends on the rows the view hides. A projection-only view keeps the optimization.

SET optimize_read_in_order = 1, optimize_aggregation_in_order = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t04813;
CREATE TABLE t04813 (key UInt64, owner String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04813 SELECT number, 'nobody' FROM numbers(10000);

CREATE VIEW v04813_invoker SQL SECURITY INVOKER AS SELECT * FROM t04813 WHERE owner != 'x';
CREATE VIEW v04813_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t04813 WHERE owner != 'x';
CREATE VIEW v04813_definer_proj DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, owner FROM t04813;

-- The `INVOKER` view stays fully optimizable: reading in order is requested.
SELECT 'invoker filtering view reads in order:', count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM v04813_invoker ORDER BY key LIMIT 3) WHERE explain LIKE '%InOrder%';

-- The filtering `DEFINER` view is a barrier: no in-order reading driven from above it.
SELECT 'definer filtering view reads in order:', count() FROM (EXPLAIN actions = 1 SELECT * FROM v04813_definer ORDER BY key LIMIT 3) WHERE explain LIKE '%InOrder%';
SELECT 'definer filtering view aggregates in order:', count() FROM (EXPLAIN actions = 1 SELECT key, count() FROM v04813_definer GROUP BY key) WHERE explain LIKE '%InOrder%';

-- A projection-only `DEFINER` view hides no rows, so it keeps the optimization.
SELECT 'projection definer view reads in order:', count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM v04813_definer_proj ORDER BY key LIMIT 3) WHERE explain LIKE '%InOrder%';

-- The barrier only drops the optimization, never the correctness of the order.
SELECT 'definer view results:', arraySort(groupArray(key)) = [0, 1, 2] FROM (SELECT key FROM v04813_definer ORDER BY key LIMIT 3);

DROP VIEW v04813_invoker;
DROP VIEW v04813_definer;
DROP VIEW v04813_definer_proj;
DROP TABLE t04813;
