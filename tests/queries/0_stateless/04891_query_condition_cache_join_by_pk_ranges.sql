-- Poisoning of the query condition cache by PK-range-sharded JOIN reads.
-- With query_plan_join_shard_by_pk_ranges = 1, the read is split into PK-range layers and a
-- border filter (FilterSortedStreamByRange) drops the other layers' rows from boundary granules.
-- A query-condition-cache-tagged filter above the scan must not record "no rows matched" for
-- such incomplete chunks: rows of the same granules in another layer may still match.
-- Before the fix, the first query wrote a poisoned cache entry and the second query read it,
-- skipped the granule, and returned 0.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (1);
INSERT INTO t2 VALUES (1), (2);

SET query_plan_join_shard_by_pk_ranges = 1;
SET use_query_condition_cache = 1;
SET max_threads = 2; -- at least two streams are needed for the layer split
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0; -- spilling joins are not sharded by PK ranges
SET enable_join_runtime_filters = 0; -- keep the condition hash identical between both queries
SET query_plan_optimize_join_order_limit = 0; -- keep t2 on the right side

SELECT count() FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id = 1;
SELECT count() FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id = 1;

DROP TABLE t1;
DROP TABLE t2;
