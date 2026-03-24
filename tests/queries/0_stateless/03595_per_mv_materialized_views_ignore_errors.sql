-- Test per-MV materialized_views_ignore_errors setting.
-- The setting can be specified in the MV definition (SETTINGS clause of SELECT)
-- to ignore errors only for that specific MV, without requiring it on the INSERT.

drop table if exists source_03595;
drop table if exists target_ok1_03595;
drop table if exists target_fail_03595;
drop table if exists target_ok2_03595;
drop view if exists mv_ok1_03595;
drop view if exists mv_fail_03595;
drop view if exists mv_ok2_03595;

create table source_03595 (id UInt64, val Int64) engine = MergeTree() order by id;
create table target_ok1_03595 (id UInt64, val Int64) engine = MergeTree() order by id;
create table target_fail_03595 (id UInt64, bad_val UInt64) engine = MergeTree() order by id;
create table target_ok2_03595 (id UInt64, val Int64) engine = MergeTree() order by id;

-- mv_ok1: always succeeds
create materialized view mv_ok1_03595 to target_ok1_03595 as select id, val from source_03595;

-- mv_fail: fails when id = 0, but has per-MV ignore errors
create materialized view mv_fail_03595 to target_fail_03595 as
    select id, toUInt64(throwIf(id = 0, 'deliberate error in mv_fail')) as bad_val
    from source_03595
    settings materialized_views_ignore_errors = 1;

-- mv_ok2: always succeeds
create materialized view mv_ok2_03595 to target_ok2_03595 as select id, val from source_03595;

-- { echoOn }

-- Case 1: Insert with no error — all MVs receive data
insert into source_03595 values (1, 100), (2, 200);
select 'source after insert 1', count() from source_03595;
select 'target_ok1 after insert 1', count() from target_ok1_03595;
select 'target_fail after insert 1', count() from target_fail_03595;
select 'target_ok2 after insert 1', count() from target_ok2_03595;

-- Case 2: Insert that triggers mv_fail error (id=0).
-- Because mv_fail has per-MV materialized_views_ignore_errors=1,
-- the INSERT should succeed and data should reach source and the OK MVs.
-- mv_fail's error is ignored; it may receive partial data.
insert into source_03595 values (3, 300), (0, 400);
select 'source after insert 2', count() from source_03595;
select 'target_ok1 after insert 2', count() from target_ok1_03595;
select 'target_ok2 after insert 2', count() from target_ok2_03595;

-- Case 3: Verify that without the per-MV setting, errors still propagate.
drop view mv_fail_03595;
create materialized view mv_fail_03595 to target_fail_03595 as
    select id, toUInt64(throwIf(id = 0, 'deliberate error in mv_fail')) as bad_val
    from source_03595;

insert into source_03595 values (4, 500), (0, 600); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Source may have the new rows (blocks written before MV error)
select 'source after insert 3 (failed)', count() from source_03595;

-- Case 4: INSERT-level setting still overrides everything
insert into source_03595 settings materialized_views_ignore_errors = 1 values (5, 700), (0, 800);
select 'source after insert 4', count() from source_03595;
select 'target_ok1 after insert 4', count() from target_ok1_03595;
select 'target_ok2 after insert 4', count() from target_ok2_03595;

-- { echoOff }

drop view mv_ok1_03595;
drop view mv_fail_03595;
drop view mv_ok2_03595;
drop table source_03595;
drop table target_ok1_03595;
drop table target_fail_03595;
drop table target_ok2_03595;
