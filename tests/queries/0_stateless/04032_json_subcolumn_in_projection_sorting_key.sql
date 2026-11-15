-- Tags: no-random-settings

create table test
(
    json JSON,
    projection prj (select json, json.a order by json.b::Int32)
)
engine = MergeTree
order by ();

insert into test (json) select '{"a" : 42, "b" : 42}';

select * from test;
