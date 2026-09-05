drop table if exists source;
drop table if exists target1;
drop table if exists target2;
drop table if exists v_heavy;
drop table if exists t_as_select;


create table source(type String) engine=MergeTree order by type;

create view v_heavy as
with nums as (select number from numbers(1e5))
select count(*) n from (select number from numbers(1e5) n1 cross join nums);

create table target1(type String) engine=MergeTree order by type;
create table target2(type String) engine=MergeTree order by type;

-- A scalar subquery in a statement that is only analyzed, never executed, must not be
-- evaluated, so `throwIf` never fires. This is checked with `throwIf` rather than with a
-- timeout, because a timeout makes the test depend on machine load.
create materialized view vm_target2 to target2 as select * from source where type='two' and (select throwIf(1));

create table t_as_select engine=MergeTree order by tuple() empty as select * from source where type='two' and (select throwIf(1));

-- But it is evaluated when the statement is actually executed.
select * from source where type='two' and (select throwIf(1)) format Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
