-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: the failpoint is process-global, so while it is armed any concurrent query
--   aggregating with `min`/`max`/`count` over a `MergeTree` table throws too, and a co-runner's
--   `SYSTEM DISABLE FAILPOINT` would silently un-arm the assertions below.
-- no-parallel-replicas: `canUseProjectionForReadingStep` declines every projection under parallel
--   reading unless `parallel_replicas_support_projection`, collapsing each `EXPLAIN` assertion to 0.

-- The implicit `minmax_count` projection stores only `min`, `max` and `count`, so a query aggregating
-- with anything else can never be served by it. Declining it before it is analyzed changes no result:
-- the EXPLAIN and value assertions below read the same with and without that short-circuit. The
-- failpoint pair is the discriminating one, because whether the analysis ran is observable no other way.

SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
-- Pin every setting the optimization's eligibility depends on: each of these disables it outright,
-- which would silently turn the EXPLAIN assertions below into a vacuous 0. Only
-- `optimize_aggregation_in_order` is randomized by the runner; the rest state the contract, because
-- reading in order bypasses the projection path and `aggregate_functions_null_for_empty` is rejected
-- by `canUseProjectionForReadingStep`.
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_unmatchable;
DROP TABLE IF EXISTS t_unmatchable_nullable_key;
DROP TABLE IF EXISTS t_unmatchable_declared;
DROP TABLE IF EXISTS t_unmatchable_stats;

-- `index_granularity` is spelled out because the runner randomizes it up to 65536: a single granule
-- over this fixture leaves no exact range, which would remove `_exact_count_projection` from the
-- filtered-count plan below. `add_minmax_index_for_numeric_columns` is spelled out so the set of
-- columns the implicit projection covers is exactly the primary key.
CREATE TABLE t_unmatchable (k UInt32, g UInt16, v Float64, n Nullable(Int64))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_unmatchable
    SELECT number, number % 100, number * 1.5, if(number % 7 = 0, NULL, number) FROM numbers(20000);

SELECT 'aggregates the projection does not store are not served by it';
SELECT count() FROM (EXPLAIN SELECT sum(v) FROM t_unmatchable) WHERE explain LIKE '%_minmax_count_projection%';
SELECT count() FROM (EXPLAIN SELECT g, avg(v) FROM t_unmatchable GROUP BY g) WHERE explain LIKE '%_minmax_count_projection%';
-- A combinator makes the name unmatchable even though the base function is one the projection stores.
SELECT count() FROM (EXPLAIN SELECT countIf(v > 0) FROM t_unmatchable) WHERE explain LIKE '%_minmax_count_projection%';

SELECT 'min over a column the projection does not cover is declined by the match, not by the name';
SELECT count() FROM (EXPLAIN SELECT min(n) FROM t_unmatchable) WHERE explain LIKE '%_minmax_count_projection%';

SELECT 'min/max/count are still served by the projection';
SELECT count() FROM (EXPLAIN SELECT min(k), max(k), count() FROM t_unmatchable) WHERE explain LIKE '%_minmax_count_projection%';
-- The trivial-count optimization answers a bare `count()` before the projection pass runs, so it is
-- disabled here to reach the projection path at all.
SELECT count() FROM (EXPLAIN SELECT count() FROM t_unmatchable SETTINGS optimize_trivial_count_query = 0) WHERE explain LIKE '%_minmax_count_projection%';
SELECT count() FROM (EXPLAIN SELECT min(k) FROM t_unmatchable) WHERE explain LIKE '%_minmax_count_projection%';

SELECT 'a wrapped min/max still reports the base name, so a Nullable key is still served';
CREATE TABLE t_unmatchable_nullable_key (k Nullable(UInt32), v Float64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, allow_nullable_key = 1;

INSERT INTO t_unmatchable_nullable_key SELECT number, number * 1.5 FROM numbers(20000);

SELECT count() FROM (EXPLAIN SELECT min(k), max(k), count() FROM t_unmatchable_nullable_key) WHERE explain LIKE '%_minmax_count_projection%';

SELECT 'a filtered count still falls back to counting exact ranges';
SELECT count() FROM (EXPLAIN SELECT count() FROM t_unmatchable WHERE k > 5000) WHERE explain LIKE '%_exact_count_projection%';

SELECT 'a query with no aggregates is unaffected';
SELECT count() FROM (EXPLAIN SELECT g FROM t_unmatchable GROUP BY g) WHERE explain LIKE '%_minmax_count_projection%';

SELECT 'the analysis is entered for min/max/count and skipped for everything else';
-- The failpoint throws where the implicit projection would be analyzed, which turns "was the
-- analysis entered" into an observable. Both directions are asserted: `sum(v)` must survive because
-- the analysis is skipped, and `min(k), max(k), count()` must still throw, which is what proves the
-- short-circuit does not decline an aggregate the projection can serve.
SYSTEM ENABLE FAILPOINT aggregate_projection_analyze_implicit_minmax;
SELECT sum(v) FROM t_unmatchable;
SELECT min(k), max(k), count() FROM t_unmatchable; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT aggregate_projection_analyze_implicit_minmax;

SELECT 'force_optimize_projection still reports exactly the queries no projection can serve';
SELECT sum(v) FROM t_unmatchable SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
SELECT min(k), max(k), count() FROM t_unmatchable SETTINGS force_optimize_projection = 1;

SELECT 'results are unchanged, projection or not';
SELECT min(k), max(k), count() FROM t_unmatchable;
SELECT min(k), max(k), count() FROM t_unmatchable SETTINGS optimize_use_implicit_projections = 0;
SELECT sum(v) FROM t_unmatchable;
SELECT sum(v) FROM t_unmatchable SETTINGS optimize_use_implicit_projections = 0;

SELECT 'a declared aggregate projection is still analyzed for an aggregate the implicit one lacks';
CREATE TABLE t_unmatchable_declared (k UInt32, g UInt16, v Float64, PROJECTION p (SELECT g, sum(v) GROUP BY g))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_unmatchable_declared SELECT number, number % 100, number * 1.5 FROM numbers(20000);

SELECT count() FROM (EXPLAIN SELECT g, sum(v) FROM t_unmatchable_declared GROUP BY g) WHERE explain LIKE '%ReadFromMergeTree (p)%';
SELECT g, sum(v) FROM t_unmatchable_declared GROUP BY g ORDER BY g LIMIT 3;

SELECT 'min/max from statistics is still reached for an aggregate the implicit projection lacks';
-- ORDER BY g keeps `k` out of the implicit projection, so min(k)/max(k) can only come from statistics.
CREATE TABLE t_unmatchable_stats (k UInt32, g UInt16, v Float64)
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0,
         auto_statistics_types = 'basic';

SET use_statistics = 1, use_statistics_for_min_max_aggregation = 1, materialize_statistics_on_insert = 1;
INSERT INTO t_unmatchable_stats SELECT number, number % 100, number * 1.5 FROM numbers(20000);

SELECT count() FROM (EXPLAIN SELECT min(k), max(k) FROM t_unmatchable_stats) WHERE explain LIKE '%_statistics_min_max_projection%';
SELECT count() FROM (EXPLAIN SELECT sum(v) FROM t_unmatchable_stats) WHERE explain LIKE '%_statistics_min_max_projection%';
SELECT min(k), max(k) FROM t_unmatchable_stats;

DROP TABLE t_unmatchable;
DROP TABLE t_unmatchable_nullable_key;
DROP TABLE t_unmatchable_declared;
DROP TABLE t_unmatchable_stats;
