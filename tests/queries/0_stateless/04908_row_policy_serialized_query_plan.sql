-- Tags: distributed

SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS rp_leaf;
DROP TABLE IF EXISTS rp_dist;
DROP TABLE IF EXISTS rp_final;
DROP TABLE IF EXISTS rp_final_dist;
DROP TABLE IF EXISTS rp_mem;
DROP TABLE IF EXISTS rp_mem_dist;

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

-- A policy on the Distributed table itself cannot be pushed into a remote read: still refused.
DROP ROW POLICY IF EXISTS rp_dist_policy ON rp_dist;
CREATE ROW POLICY rp_dist_policy ON rp_dist FOR SELECT USING y < 5 TO ALL;
SELECT count() FROM rp_dist SETTINGS serialize_query_plan = 1; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY rp_dist_policy ON rp_dist;

-- Read limits still apply to the re-planned read.
SELECT count() FROM rp_dist SETTINGS serialize_query_plan = 1, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

-- FINAL keeps deduplicating before a policy on a non-sorting-key column is applied, under both
-- values of apply_row_policy_after_final, exactly as on the non-serialized path.
CREATE TABLE rp_final (k UInt32, v UInt32) ENGINE = ReplacingMergeTree ORDER BY k;
-- One row per key, so the result does not depend on which duplicate FINAL keeps
-- (that choice varies with optimize_on_insert, which CI randomizes).
INSERT INTO rp_final VALUES (1, 9), (2, 2), (3, 7);
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
