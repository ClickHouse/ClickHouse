-- Tags: no-parallel
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge writes its result with the column names of the current metadata, so it materializes a
-- pending `RENAME COLUMN` by itself. The merged part has to record that. Otherwise the rename
-- mutation stays pending for it, resolves the new name back to the physical name the merge already
-- replaced, reads the column as missing and fills defaults - losing every value of a column that has
-- no default expression to refill from.
-- https://github.com/ClickHouse/ClickHouse/issues/111001

set allow_experimental_dynamic_type = 1;

drop table if exists t_rename_materialized_by_merge;

-- Compact parts: there the rename mutation rewrites the whole part through the reader, so a rename
-- applied twice reads the column as missing. In a Wide part the mutation only renames files, which
-- is a no-op once the merge already wrote the new name, so the loss does not show there.
create table t_rename_materialized_by_merge (x UInt64, y UInt64)
engine = MergeTree order by x
settings min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000;

insert into t_rename_materialized_by_merge select number, number from numbers(3);

-- `Dynamic` cannot carry a default expression, so the loss is unavoidable rather than merely
-- observable. Any type without a default reproduces it.
alter table t_rename_materialized_by_merge add column d Dynamic settings mutations_sync = 1;

insert into t_rename_materialized_by_merge select number, number, number from numbers(3, 3);
insert into t_rename_materialized_by_merge select number, number, 'str_' || toString(number) from numbers(6, 3);

-- Keep the rename mutation unselected, so the merge below straddles it: the metadata already carries
-- `d1` while every part still physically stores `d`.
system enable failpoint mt_select_parts_to_mutate_no_free_threads;

alter table t_rename_materialized_by_merge rename column d to d1 settings alter_sync = 0;

optimize table t_rename_materialized_by_merge final;

-- A merge has to have happened, otherwise the rest asserts nothing.
select 'merged into', count() from system.parts
where database = currentDatabase() and table = 't_rename_materialized_by_merge' and active;

select 'after merge';
select count(), dynamicType(d1) from t_rename_materialized_by_merge group by dynamicType(d1) order by count(), dynamicType(d1);

system disable failpoint mt_select_parts_to_mutate_no_free_threads;

-- `RENAME COLUMN` is a barrier command, so this waits out the pending rename mutation before
-- registering its own: past this point both renames are materialized.
alter table t_rename_materialized_by_merge rename column d1 to d2 settings mutations_sync = 2;

select 'after mutation';
select count(), dynamicType(d2) from t_rename_materialized_by_merge group by dynamicType(d2) order by count(), dynamicType(d2);
select x, y, d2 from t_rename_materialized_by_merge order by x;

drop table t_rename_materialized_by_merge;
