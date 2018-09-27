set allow_experimental_low_cardinality_type = 1;

drop table if exists test.lc;
create table test.lc (str StringWithDictionary, val UInt8WithDictionary) engine = MergeTree order by tuple();
insert into test.lc values ('a', 1), ('b', 2);
select str, str in ('a', 'd') from test.lc;
select val, val in (1, 3) from test.lc;
select str, str in (select arrayJoin(['a', 'd'])) from test.lc;
select val, val in (select arrayJoin([1, 3])) from test.lc;
select str, str in (select str from test.lc) from test.lc;
select val, val in (select val from test.lc) from test.lc;
drop table if exists test.lc;
