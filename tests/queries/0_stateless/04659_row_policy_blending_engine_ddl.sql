-- On engines which merge rows into one, a row policy needs an opt-in

CREATE TABLE t_summing (tenant String, key String, secret UInt64) ENGINE = SummingMergeTree ORDER BY (tenant, key);
CREATE TABLE t_aggregating (tenant String, total AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY tenant;
CREATE TABLE t_coalescing (tenant String, secret Nullable(String)) ENGINE = CoalescingMergeTree ORDER BY tenant;
CREATE TABLE t_replacing (tenant String, secret UInt64) ENGINE = ReplacingMergeTree ORDER BY tenant;

CREATE ROW POLICY p_blend ON t_summing USING secret < 100 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p_blend ON t_aggregating USING finalizeAggregation(total) < 100 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p_blend ON t_coalescing USING secret != 'hidden' TO ALL; -- { serverError BAD_ARGUMENTS }

-- a filter over the sorting key is safe, but the check does not look at the filter
CREATE ROW POLICY p_blend ON t_summing USING tenant = 'me' TO ALL; -- { serverError BAD_ARGUMENTS }

-- engines which pick a single winning row are not affected
CREATE ROW POLICY p_key ON t_replacing USING secret < 100 TO ALL;
DROP ROW POLICY p_key ON t_replacing;

SET allow_suspicious_row_policies_with_blending_engines = 1;
CREATE ROW POLICY p_blend ON t_summing USING secret < 100 TO ALL;

-- ALTER is checked too
SET allow_suspicious_row_policies_with_blending_engines = 0;
ALTER ROW POLICY p_blend ON t_summing USING secret < 200; -- { serverError BAD_ARGUMENTS }

SELECT short_name, table, select_filter FROM system.row_policies WHERE database = currentDatabase() ORDER BY table;

-- DROP always works
DROP ROW POLICY p_blend ON t_summing;
