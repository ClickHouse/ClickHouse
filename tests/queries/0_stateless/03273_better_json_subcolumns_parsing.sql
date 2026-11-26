SET enable_json_type = 1;
drop table if exists test;
create table test (json JSON) engine=Memory;
insert into test format JSONAsObject {"a" : 42}, {"a" : 42.42}, {"a" : 43};

select dynamicType(json.a), json.a from test;
drop table test;
