-- Tags: no-parallel
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge extends the merged part's data version through the pending mutations it materializes by
-- itself. `CLEAR COLUMN` is not one of them: it is a `DROP_COLUMN` command with `clear`, and it must
-- stay pending for the merged part, so that it is applied to it later.
-- https://github.com/ClickHouse/ClickHouse/issues/111001

drop table if exists t_clear_materialized_by_merge;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` leaves the merge to `OPTIMIZE ... FINAL`, which
-- ignores that limit, so the part name asserted below is the result of that one merge and not of a
-- background merge racing it - a background merge of the two parts followed by `OPTIMIZE ... FINAL`
-- rewriting the single result would produce `all_1_2_2` instead.
create table t_clear_materialized_by_merge (x UInt64, y UInt64, s String)
engine = MergeTree order by x
settings min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1;

insert into t_clear_materialized_by_merge select number, number, 'str_' || toString(number) from numbers(3);
insert into t_clear_materialized_by_merge select number, number, 'str_' || toString(number) from numbers(3, 3);

-- Keep the mutation unselected, so the merge below straddles it.
system enable failpoint mt_select_parts_to_mutate_no_free_threads;

alter table t_clear_materialized_by_merge clear column y settings alter_sync = 0;

optimize table t_clear_materialized_by_merge final;

-- A merge has to have happened, otherwise the rest asserts nothing. The merged part must not
-- record the pending clear as applied: its data version stays below the mutation version, so the
-- part is `all_1_2_1` with data version 1, not `all_1_2_1_3`.
select 'merged into', name, data_version from system.parts
where database = currentDatabase() and table = 't_clear_materialized_by_merge' and active;

select 'after merge';
select x, y, s from t_clear_materialized_by_merge order by x;

system disable failpoint mt_select_parts_to_mutate_no_free_threads;

-- Mutations are applied in order, so waiting for this one waits out the pending clear as well.
alter table t_clear_materialized_by_merge clear column s settings mutations_sync = 2;

select 'after mutation';
select x, y, s from t_clear_materialized_by_merge order by x;

drop table t_clear_materialized_by_merge;
