-- Tags: no-parallel

-- Fail-closed guards for the experimental per-part aggregation cache: the optimization must not
-- reuse stale per-part states across metadata `ALTER`, lightweight deletes, overflow-mode limits,
-- and must not run (read parts / mutate the cache) during `EXPLAIN`.

-- Pin `max_rows_to_group_by = 0` at the session level: the functional-test config
-- (`tests/config/users.d/limits.yaml`) sets it to `10G`, and a non-zero value makes every query fail
-- closed out of the optimization, so the positive guards below would never populate the cache. The
-- overflow-mode guard further down overrides it per query to exercise the skip path explicitly.
-- The same config sets read limits (`max_rows_to_read = 20000000`, `max_bytes_to_read`/`max_bytes_to_read_leaf = 1T`),
-- which also fail the optimization closed (the cache serves aggregate states, not raw rows, so it cannot honor
-- them); pin them to 0 too so the positive guards populate the cache. The read-limit guard below sets one
-- explicitly per query to exercise its skip path.
SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_pac_failclose;
CREATE TABLE t_pac_failclose (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_failclose VALUES (1, 10), (2, 20);
INSERT INTO t_pac_failclose VALUES (1, 40), (2, 50);

-- `EXPLAIN` must not populate the cache (static planning only). Consume the plan rows via a
-- subquery so the test does not depend on the textual plan format.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT k, sum(v) FROM t_pac_failclose GROUP BY k);
SELECT count() FROM system.part_aggregation_cache;

-- Metadata-only `ALTER MODIFY COLUMN` must not reuse a state cached for the old column type.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k;
SELECT count() FROM system.part_aggregation_cache;
ALTER TABLE t_pac_failclose MODIFY COLUMN v UInt32 SETTINGS mutations_sync = 2;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k;

-- `group_by_overflow_mode = 'any'` with `max_rows_to_group_by` must skip the optimization
-- (per-part limits would not match the single-pass semantics), so nothing is cached.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() FROM (SELECT k, sum(v) FROM t_pac_failclose GROUP BY k SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any');
SELECT count() FROM system.part_aggregation_cache;

-- A read limit (`max_rows_to_read` / `max_bytes_to_read`) must skip the optimization: the cached source
-- emits aggregate states, not the raw rows these limits count, and the populator's own reads do not feed
-- them either, so the cache cannot honor the limit. A cold query with a (non-exceeded) limit set must
-- therefore not populate the cache.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() FROM (SELECT k, sum(v) FROM t_pac_failclose GROUP BY k SETTINGS max_bytes_to_read = 100000000);
SELECT count() FROM system.part_aggregation_cache;

-- And even with a warm cache the limit must still be enforced by the normal read path rather than
-- bypassed by reusing cached states: re-running with a tight `max_rows_to_read` must throw.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k SETTINGS max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

-- A lightweight delete must not reuse states cached before the delete.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k;
DELETE FROM t_pac_failclose WHERE v = 10;
SELECT k, sum(v) FROM t_pac_failclose GROUP BY k ORDER BY k;

DROP TABLE t_pac_failclose;
SYSTEM DROP PART AGGREGATION CACHE;
