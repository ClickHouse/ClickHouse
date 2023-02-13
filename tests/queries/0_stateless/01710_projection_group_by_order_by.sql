-- Tags: no-s3-storage

DROP TABLE IF EXISTS t;
drop table if exists tp;

create table tp (type Int32, eventcnt UInt64, projection p (select sum(eventcnt), type group by type order by sum(eventcnt))) engine = MergeTree order by type; -- { serverError 583 }

drop table if exists tp;
