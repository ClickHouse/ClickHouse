drop table if exists final_row_policy SYNC;
create table final_row_policy (a UInt64, b UInt64, c UInt64, is_deleted UInt8, version UInt64) engine = ReplacingMergeTree(version) order by (a, b, c);

insert into final_row_policy select 1, 1, 1, 0, 1;
insert into final_row_policy select 1, 1, 1, 1, 2;

DROP ROW POLICY IF EXISTS not_deleted ON final_row_policy;
CREATE ROW POLICY not_deleted ON final_row_policy AS RESTRICTIVE FOR SELECT USING (is_deleted = 0) TO ALL;

-- { echo ON }
-- By default, POW POLICY is applied after final. Result should be empty.
select * from final_row_policy FINAL;

-- Setting optimize_move_to_prewhere_if_final does not affect the result anymore.
select * from final_row_policy FINAL settings apply_row_policy_after_final = 0, optimize_move_to_prewhere_if_final=1;
select * from final_row_policy FINAL settings apply_row_policy_after_final = 0, optimize_move_to_prewhere_if_final=0;
select * from final_row_policy FINAL settings apply_row_policy_after_final = 1, optimize_move_to_prewhere_if_final=1;
select * from final_row_policy FINAL settings apply_row_policy_after_final = 1, optimize_move_to_prewhere_if_final=0;
