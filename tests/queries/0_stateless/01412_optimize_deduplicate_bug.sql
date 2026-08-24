drop table if exists tesd_dedupl;

create table tesd_dedupl (x UInt32, y UInt32) engine = MergeTree order by x;
insert into tesd_dedupl values (1, 1);
insert into tesd_dedupl values (1, 1);

OPTIMIZE TABLE tesd_dedupl DEDUPLICATE;
select * from tesd_dedupl;

drop table if exists tesd_dedupl;
