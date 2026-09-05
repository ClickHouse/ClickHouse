-- Tags: no-parallel
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge writes its result with the column names of the current metadata, so it materializes a
-- pending `RENAME COLUMN` by itself, and the merged part records that in its data version. A part
-- carries a single data version, so it can only record a contiguous prefix of the pending mutations
-- as applied: with a mutation the merge does not materialize in front of the rename, the merge has
-- to be refused instead. Otherwise the rename is applied to the merged part a second time, resolves
-- the new name back to the physical name the merge already replaced, reads the column as missing and
-- fills defaults - losing every value of a column that has no default expression to refill from.
-- https://github.com/ClickHouse/ClickHouse/issues/111001

-- The shape that reaches the refusal is a single `ALTER` combining a command the merge does not
-- materialize with a `RENAME COLUMN`: both are metadata alters, so they end up in one mutation
-- entry, and `RENAME COLUMN` being a barrier command only makes `alter` wait for the mutations
-- registered *before* it. Two separate `ALTER` queries do not reach it - the rename waits out every
-- earlier mutation - and neither does `MATERIALIZE COLUMN x, RENAME COLUMN d TO d1`, because
-- `MATERIALIZE COLUMN` is not a metadata alter: `InterpreterAlterQuery` puts it in a segment of its
-- own that registers first, and the rename then waits for it.

set allow_experimental_dynamic_type = 1;

drop table if exists t_merge_refused_before_pending_rename;

-- Compact parts, as in `05037_rename_column_materialized_by_merge`: there a rename mutation rewrites
-- the whole part through the reader, so a rename applied twice reads the column as missing. In a Wide
-- part it only renames files, which is a no-op once the merge has written the new name already.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` leaves the merges of this test to `OPTIMIZE ...
-- FINAL`, which ignores that limit, so that every part name below is the result of one known merge
-- rather than of a background merge racing it.
create table t_merge_refused_before_pending_rename (x UInt64, y UInt64, w UInt64, d Dynamic)
engine = MergeTree order by x
settings min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1;

insert into t_merge_refused_before_pending_rename select number, number, number, number from numbers(3);
insert into t_merge_refused_before_pending_rename select number, number, number, 'str_' || toString(number) from numbers(3, 3);

-- Keep the mutation unselected, so that it is still pending when the merge below is selected.
system enable failpoint mt_select_parts_to_mutate_no_free_threads;

alter table t_merge_refused_before_pending_rename clear column y, rename column d to d1 settings alter_sync = 0;

-- Both commands are in one mutation entry, which is what the refusal is about: `system.mutations`
-- has a row per command, and here both rows carry the same `mutation_id`. The merge does not
-- materialize the clear - it has to stay pending for the merged part - so the merge cannot record
-- the rename behind it either.
select 'pending entry', uniqExact(mutation_id), count(),
    countIf(command like '%CLEAR COLUMN%'), countIf(command like '%RENAME COLUMN%')
from system.mutations
where database = currentDatabase() and table = 't_merge_refused_before_pending_rename' and not is_done;

optimize table t_merge_refused_before_pending_rename final settings optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

-- The merge really did not happen: the source parts are still the active ones.
select 'active parts', name from system.parts
where database = currentDatabase() and table = 't_merge_refused_before_pending_rename' and active
order by name;

system disable failpoint mt_select_parts_to_mutate_no_free_threads;

-- Mutations are applied in order, so waiting for this one waits out the pending entry as well.
alter table t_merge_refused_before_pending_rename update w = w where 1 settings mutations_sync = 2;

-- With the rename applied, nothing is left for the merge to materialize and it is allowed again.
optimize table t_merge_refused_before_pending_rename final settings optimize_throw_if_noop = 1;

select 'merged into', name from system.parts
where database = currentDatabase() and table = 't_merge_refused_before_pending_rename' and active;

-- The rename was applied exactly once, so the values of `d1` are all there.
select count(), dynamicType(d1) from t_merge_refused_before_pending_rename
group by dynamicType(d1) order by count(), dynamicType(d1);

select x, y, d1 from t_merge_refused_before_pending_rename order by x;

drop table t_merge_refused_before_pending_rename;
