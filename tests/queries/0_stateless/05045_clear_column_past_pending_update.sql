-- Tags: no-parallel
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge materializes a pending `CLEAR COLUMN` on its own: `AlterConversions` reports the column as
-- dropped while the mutation is pending, so the merge writes defaults for it. The merged part carries
-- a single data version, so it cannot record that while an earlier mutation is still pending - and
-- that earlier mutation, once it runs, reads the already cleared values. Such a merge must be
-- refused, so that `z` is computed from the values that predate the clear.
-- https://github.com/ClickHouse/ClickHouse/issues/111001

drop table if exists t_clear_past_pending_update;

create table t_clear_past_pending_update (x UInt64, y UInt64, z UInt64)
engine = MergeTree order by x
settings min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000;

insert into t_clear_past_pending_update select number, number, 0 from numbers(3);
insert into t_clear_past_pending_update select number, number, 0 from numbers(3, 3);

-- Keep both mutations unselected, so that the merge below straddles them.
system enable failpoint mt_select_parts_to_mutate_no_free_threads;

-- Only `RENAME COLUMN` is a barrier command, so the clear does not wait out the update: both are
-- pending at once, and the merge would materialize the second one ahead of the first.
alter table t_clear_past_pending_update update z = y + 100 where 1 settings mutations_sync = 0;
alter table t_clear_past_pending_update clear column y settings alter_sync = 0;

optimize table t_clear_past_pending_update final;

-- The merge has to be refused: the parts stay as they are, with data version 0.
select 'parts', name, data_version from system.parts
where database = currentDatabase() and table = 't_clear_past_pending_update' and active
order by name;

system disable failpoint mt_select_parts_to_mutate_no_free_threads;

-- Mutations are applied in order, so waiting for this one waits out both pending mutations.
alter table t_clear_past_pending_update update x = x where 1 settings mutations_sync = 2;

-- `z` keeps the values `y` had before the clear.
select 'after mutation';
select x, y, z from t_clear_past_pending_update order by x;

drop table t_clear_past_pending_update;
