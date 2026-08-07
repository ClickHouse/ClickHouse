drop table if exists tp;

create table tp (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y;

alter table tp drop projection pp; -- { serverError NO_SUCH_PROJECTION_IN_TABLE }
alter table tp drop projection if exists pp;
alter table tp drop projection if exists p;
alter table tp drop projection p;  -- { serverError NO_SUCH_PROJECTION_IN_TABLE }
alter table tp drop projection if exists p;

drop table tp;
