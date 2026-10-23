-- Tags: no-parallel
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge extends the merged part's data version through the pending mutations it materializes by
-- itself. A mutation command scoped to other partitions leaves nothing pending for a part of this
-- partition - `MutateTask` skips it and clones the untouched part forward to the mutation version -
-- so it must not stop that scan. `IN PARTITION p` and `IN PARTITION p1, p2, ...` are stored in two
-- different fields of the parsed command, and both have to be honoured here, just as
-- `canSkipMutationCommandForPart` honours them.
-- https://github.com/ClickHouse/ClickHouse/issues/111001

drop table if exists t_merge_past_multi_partition_update;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` leaves the merge to `OPTIMIZE ... FINAL`, which
-- ignores that limit, so the part name asserted below is the result of that one merge and not of a
-- background merge racing it - a background merge of the two parts followed by `OPTIMIZE ... FINAL`
-- rewriting the single result would produce `3_3_4_2_6` instead.
create table t_merge_past_multi_partition_update (p UInt64, x UInt64, y UInt64, s String)
engine = MergeTree partition by p order by x
settings min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1;

insert into t_merge_past_multi_partition_update select 1, number, number, 'str_' || toString(number) from numbers(3);
insert into t_merge_past_multi_partition_update select 2, number, number, 'str_' || toString(number) from numbers(3);
insert into t_merge_past_multi_partition_update select 3, number, number, 'str_' || toString(number) from numbers(3);
insert into t_merge_past_multi_partition_update select 3, number, number, 'str_' || toString(number) from numbers(3, 3);

-- Keep both mutations unselected, so that the merge below straddles them.
system enable failpoint mt_select_parts_to_mutate_no_free_threads;

-- The update applies to partitions 1 and 2 only. `UPDATE` is not a barrier command, so the metadata
-- mutation below does not wait for it: both are pending at once.
alter table t_merge_past_multi_partition_update update y = y + 100 in partition 1, 2 where 1 settings mutations_sync = 0;
alter table t_merge_past_multi_partition_update drop column s settings alter_sync = 0;

optimize table t_merge_past_multi_partition_update partition 3 final;

-- The merge in partition 3 has to happen, and the merged part has to record both mutations as
-- applied: the update is irrelevant for this partition, and the drop is materialized by the merge
-- itself. So the merged part is `3_3_4_1_6` with data version 6, not `3_3_4_1` with data version 4.
select 'merged into', name, data_version from system.parts
where database = currentDatabase() and table = 't_merge_past_multi_partition_update' and active and partition = '3';

select 'after merge';
select p, x, y from t_merge_past_multi_partition_update order by p, x;

system disable failpoint mt_select_parts_to_mutate_no_free_threads;

-- Mutations are applied in order, so waiting for this one waits out both pending mutations.
alter table t_merge_past_multi_partition_update update y = y where 1 settings mutations_sync = 2;

select 'after mutation';
select p, x, y from t_merge_past_multi_partition_update order by p, x;

drop table t_merge_past_multi_partition_update;
