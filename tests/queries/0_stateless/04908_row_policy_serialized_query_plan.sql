-- Tags: distributed

SET prefer_localhost_replica = 0;
-- The initiator only ships a serialized plan under the analyzer, so without this every arm
-- below is unable to tell a filtered read from an unfiltered one.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS rp_leaf;
DROP TABLE IF EXISTS rp_dist;
DROP TABLE IF EXISTS rp_final;
DROP TABLE IF EXISTS rp_final_dist;
DROP TABLE IF EXISTS rp_mem;
DROP TABLE IF EXISTS rp_mem_dist;
DROP TABLE IF EXISTS rp_mem_nd;
DROP TABLE IF EXISTS rp_mem_nd_dist;

CREATE TABLE rp_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_leaf SELECT number, number FROM numbers(10);
CREATE TABLE rp_dist AS rp_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_leaf);

DROP ROW POLICY IF EXISTS rp_leaf_policy ON rp_leaf;
CREATE ROW POLICY rp_leaf_policy ON rp_leaf FOR SELECT USING y < 5 TO ALL;

SELECT 'local', count() FROM rp_leaf;

-- The policy column is not in the select list, so the read must be widened with it on the
-- executing node. Every one of these returned all 10 rows before, or failed to find `y`.
SELECT 'count sqp=0', count() FROM rp_dist SETTINGS serialize_query_plan = 0;
SELECT 'count sqp=1', count() FROM rp_dist SETTINGS serialize_query_plan = 1;
SELECT 'count sqp=1 no trivial', count() FROM rp_dist SETTINGS serialize_query_plan = 1, optimize_trivial_count_query = 0;
SELECT 'sum(x) sqp=1', sum(x) FROM rp_dist SETTINGS serialize_query_plan = 1;
SELECT 'sum(y) sqp=1', sum(y) FROM rp_dist SETTINGS serialize_query_plan = 1;
SELECT 'subquery sqp=1', count() FROM (SELECT x FROM rp_dist) SETTINGS serialize_query_plan = 1;
SELECT 'prewhere sqp=1', count() FROM rp_dist PREWHERE x > 1 SETTINGS serialize_query_plan = 1;

SELECT 'distinct sqp=1', y FROM rp_dist GROUP BY y ORDER BY y SETTINGS serialize_query_plan = 1;

-- Whether these reads really were shipped as a serialized plan, rather than silently falling back
-- to query text, is asserted in 04909_row_policy_serialized_plan_route.sh: it needs a retry loop
-- around the executing node's log row, which this file cannot express.

-- A policy on the Distributed table itself cannot be pushed into a remote read: still refused.
DROP ROW POLICY IF EXISTS rp_dist_policy ON rp_dist;
CREATE ROW POLICY rp_dist_policy ON rp_dist FOR SELECT USING y < 5 TO ALL;
SELECT count() FROM rp_dist SETTINGS serialize_query_plan = 1; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY rp_dist_policy ON rp_dist;

-- Read limits still apply to the re-planned read.
SELECT count() FROM rp_dist SETTINGS serialize_query_plan = 1, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

-- A policy containing an IN subquery registers a set while its filter is built. The re-planned read
-- stops at FetchColumns, before the planner would add the step that builds that set, so the set has
-- to be built here or execution reaches function `in` with a not-ready set.
-- The subquery reads system.numbers: a policy is stored as text and re-resolved on the executing
-- node against its own default database, so an unqualified per-test table would not resolve there,
-- and the database name is not expressible in a .sql test.
-- rp_leaf_policy also applies (policies conjoin), so drop it to isolate this arm.
DROP ROW POLICY rp_leaf_policy ON rp_leaf;
DROP ROW POLICY IF EXISTS rp_sub_policy ON rp_leaf;
CREATE ROW POLICY rp_sub_policy ON rp_leaf FOR SELECT USING x IN (SELECT number * 3 FROM system.numbers LIMIT 4) TO ALL;
SELECT 'subq policy local', arraySort(groupArray(x)) FROM rp_leaf;
SELECT 'subq policy sqp=1', arraySort(groupArray(x)) FROM rp_dist SETTINGS serialize_query_plan = 1;
SELECT 'subq policy sqp=0', arraySort(groupArray(x)) FROM rp_dist SETTINGS serialize_query_plan = 0;
DROP ROW POLICY rp_sub_policy ON rp_leaf;
CREATE ROW POLICY rp_leaf_policy ON rp_leaf FOR SELECT USING y < 5 TO ALL;

-- A policy on a non-sorting-key column interacts with FINAL, so for each value of
-- apply_row_policy_after_final the serialized read must agree with the non-serialized one.
CREATE TABLE rp_final (k UInt32, v UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
-- Two rows per key with distinct versions, in separate parts: the row FINAL keeps is the
-- max-version one. The policy reads `v`, a non-sorting-key column, and the winning row's `v`
-- decides whether the key survives, so applying the policy before FINAL gives a different
-- answer than applying it after. Each part holds one row per key, so the rows on disk do not
-- depend on optimize_on_insert, which CI randomizes and which only dedups within one block.
INSERT INTO rp_final VALUES (1, 9, 1), (2, 2, 1), (3, 7, 1);
INSERT INTO rp_final VALUES (1, 2, 2), (2, 9, 2), (3, 3, 2);
CREATE TABLE rp_final_dist AS rp_final ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_final);
DROP ROW POLICY IF EXISTS rp_final_policy ON rp_final;
CREATE ROW POLICY rp_final_policy ON rp_final FOR SELECT USING v < 5 TO ALL;

SELECT 'final after=1 sqp=1', k, v FROM rp_final_dist FINAL ORDER BY k, v SETTINGS serialize_query_plan = 1, apply_row_policy_after_final = 1;
SELECT 'final after=1 sqp=0', k, v FROM rp_final_dist FINAL ORDER BY k, v SETTINGS serialize_query_plan = 0, apply_row_policy_after_final = 1;
SELECT 'final after=0 sqp=1', k, v FROM rp_final_dist FINAL ORDER BY k, v SETTINGS serialize_query_plan = 1, apply_row_policy_after_final = 0;
SELECT 'final after=0 sqp=0', k, v FROM rp_final_dist FINAL ORDER BY k, v SETTINGS serialize_query_plan = 0, apply_row_policy_after_final = 0;

-- A storage that does not support PREWHERE gets the policy as a filter step of the shipped plan.
-- That read must keep taking the direct-read path, or the policy would be applied twice.
CREATE TABLE rp_mem (x UInt32, y UInt32) ENGINE = Memory;
INSERT INTO rp_mem SELECT number, number FROM numbers(10);
CREATE TABLE rp_mem_dist AS rp_mem ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_mem);
DROP ROW POLICY IF EXISTS rp_mem_policy ON rp_mem;
CREATE ROW POLICY rp_mem_policy ON rp_mem FOR SELECT USING y < 5 TO ALL;
SELECT 'memory sqp=1', count() FROM rp_mem_dist SETTINGS serialize_query_plan = 1;
SELECT 'memory sqp=0', count() FROM rp_mem_dist SETTINGS serialize_query_plan = 0;

-- A deterministic policy returns the same count whether it is applied once or twice, so the
-- exactly-once property needs a policy that keeps a different subset per evaluation: applied
-- once it keeps about half the rows, twice about a quarter. Classify instead of comparing raw
-- counts, which vary per run.
CREATE TABLE rp_mem_nd (x UInt32) ENGINE = Memory;
INSERT INTO rp_mem_nd SELECT number FROM numbers(100000);
CREATE TABLE rp_mem_nd_dist AS rp_mem_nd ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_mem_nd);
DROP ROW POLICY IF EXISTS rp_mem_nd_policy ON rp_mem_nd;
CREATE ROW POLICY rp_mem_nd_policy ON rp_mem_nd FOR SELECT USING rand64() % 2 = 0 TO ALL;
SELECT 'memory nd sqp=1', if(abs(count() - 50000) < 2000, 'ONCE', if(abs(count() - 25000) < 2000, 'TWICE', 'OTHER'))
FROM rp_mem_nd_dist SETTINGS serialize_query_plan = 1;
SELECT 'memory nd sqp=0', if(abs(count() - 50000) < 2000, 'ONCE', if(abs(count() - 25000) < 2000, 'TWICE', 'OTHER'))
FROM rp_mem_nd_dist SETTINGS serialize_query_plan = 0;
DROP ROW POLICY rp_mem_nd_policy ON rp_mem_nd;

-- max_columns_to_read counts the columns the query selects. The policy's own columns are read
-- besides those, so the limit must not see them, whichever way the read reached the node.
SELECT 'maxcols sqp=1', count() FROM rp_dist SETTINGS serialize_query_plan = 1, max_columns_to_read = 1;
SELECT 'maxcols sqp=0', count() FROM rp_dist SETTINGS serialize_query_plan = 0, max_columns_to_read = 1;
-- The limit still rejects a selection that genuinely exceeds it, which master also rejects here.
SELECT x, y FROM rp_dist ORDER BY x
SETTINGS serialize_query_plan = 1, max_columns_to_read = 1; -- { serverError TOO_MANY_COLUMNS }
SELECT x, y FROM rp_dist ORDER BY x
SETTINGS serialize_query_plan = 0, max_columns_to_read = 1; -- { serverError TOO_MANY_COLUMNS }

DROP ROW POLICY rp_leaf_policy ON rp_leaf;
DROP ROW POLICY rp_final_policy ON rp_final;
DROP ROW POLICY rp_mem_policy ON rp_mem;

-- Without a policy the read is unchanged.
SELECT 'no policy sqp=1', count() FROM rp_dist SETTINGS serialize_query_plan = 1;
SELECT 'no policy sqp=0', count() FROM rp_dist SETTINGS serialize_query_plan = 0;

DROP TABLE rp_dist;
DROP TABLE rp_leaf;
DROP TABLE rp_final_dist;
DROP TABLE rp_final;
DROP TABLE rp_mem_dist;
DROP TABLE rp_mem;
DROP TABLE rp_mem_nd_dist;
DROP TABLE rp_mem_nd;
