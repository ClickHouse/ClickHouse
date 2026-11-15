drop table if exists tp;

create table tp (d1 Int32, d2 Int32, eventcnt Int64, projection p (select sum(eventcnt) group by d1)) engine = MergeTree order by (d1, d2);

set optimize_use_projections = 1, force_optimize_projection = 1;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

select sum(eventcnt) eventcnt, d1 from tp group by d1;

select avg(eventcnt) eventcnt, d1 from tp group by d1;

insert into tp values (1, 2, 3);

select sum(eventcnt) eventcnt, d1 from tp group by d1;

select avg(eventcnt) eventcnt, d1 from tp group by d1; -- { serverError PROJECTION_NOT_USED }

drop table tp;
