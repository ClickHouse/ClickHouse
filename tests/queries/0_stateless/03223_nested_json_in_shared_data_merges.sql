set allow_experimental_json_type = 1;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
insert into test select materialize('{"a" : [{"b" : 42}]}')::JSON(max_dynamic_paths=8) from numbers(5);
insert into test select materialize('{"aa1" : 42, "aa2" : 42, "aa3" : 42, "aa4" : 42, "aa5" : 42, "aa6" : 42, "aa7" : 42, "aa8" : 42, "a" : [{"c" : 42}]}') from numbers(5);
optimize table test final;

select 'All paths';
select JSONAllPaths(arrayJoin(json.a[])) from test;
select 'Dynamic paths';
select JSONDynamicPaths(arrayJoin(json.a[])) from test;
select 'Shared data paths';
select JSONSharedDataPaths(arrayJoin(json.a[])) from test;

insert into test select materialize('{"a" : [{"b" : 42}]}')::JSON(max_dynamic_paths=8) from numbers(5);
optimize table test final;

select 'All paths';
select JSONAllPaths(arrayJoin(json.a[])) from test;
select 'Dynamic paths';
select JSONDynamicPaths(arrayJoin(json.a[])) from test;
select 'Shared data paths';
select JSONSharedDataPaths(arrayJoin(json.a[])) from test;

drop table test;
