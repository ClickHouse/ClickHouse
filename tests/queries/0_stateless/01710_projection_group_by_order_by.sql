--Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: bug in formatting of projections.
-- https://github.com/ClickHouse/ClickHouse/issues/44318

DROP TABLE IF EXISTS t;
drop table if exists tp;

create table tp (type Int32, eventcnt UInt64, projection p (select sum(eventcnt), type group by type order by sum(eventcnt))) engine = MergeTree order by type; -- { serverError ILLEGAL_PROJECTION }

drop table if exists tp;
