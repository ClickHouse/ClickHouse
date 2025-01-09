-- Tags: long

set allow_experimental_dynamic_type = 1;
set allow_experimental_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set max_block_size = 10000;

drop table if exists test_json_dynamic_aggregate_functions;
create table test_json_dynamic_aggregate_functions (json JSON(a1 String, max_dynamic_paths=2, max_dynamic_types=2)) engine=Memory;
insert into test_json_dynamic_aggregate_functions select toJSONString(map('a' || number % 13, multiIf(number % 5 == 0, NULL, number % 5 == 1, number::UInt32, number % 5 == 2, 'str_' || number, number % 5 == 3, range(number % 5), toBool(number % 2)))) from numbers(100000);
select arrayJoin(distinctJSONPaths(json)) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctJSONPathsAndTypes(json)) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctDynamicTypes(json.a2)) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctDynamicTypes(json.a3)) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctDynamicTypes(json.a42)) from test_json_dynamic_aggregate_functions;

select 'Filter';
select arrayJoin(distinctJSONPaths(json)) from test_json_dynamic_aggregate_functions where dynamicType(json.a2) == 'String';
select arrayJoin(distinctJSONPathsAndTypes(json)) from test_json_dynamic_aggregate_functions where dynamicType(json.a2) == 'String';
select arrayJoin(distinctDynamicTypes(json.a2)) from test_json_dynamic_aggregate_functions where dynamicType(json.a2) == 'String';

select 'If';
select arrayJoin(distinctJSONPathsIf(json, dynamicType(json.a2) == 'String')) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctJSONPathsAndTypesIf(json, dynamicType(json.a2) == 'String')) from test_json_dynamic_aggregate_functions;
select arrayJoin(distinctDynamicTypesIf(json.a2, dynamicType(json.a2) == 'String')) from test_json_dynamic_aggregate_functions;

select 'Group by';
select dynamicType(json.a2), distinctJSONPaths(json) from test_json_dynamic_aggregate_functions group by dynamicType(json.a2) order by dynamicType(json.a2);
select dynamicType(json.a2), distinctJSONPathsAndTypes(json) from test_json_dynamic_aggregate_functions group by dynamicType(json.a2) order by dynamicType(json.a2);
select dynamicType(json.a2), distinctDynamicTypes(json.a2) from test_json_dynamic_aggregate_functions group by dynamicType(json.a2) order by dynamicType(json.a2);

select 'Remote';
select arrayJoin(distinctJSONPaths(json)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);
select arrayJoin(distinctJSONPathsAndTypes(json)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);
select arrayJoin(distinctDynamicTypes(json.a2)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);

select 'Remote filter';
select arrayJoin(distinctJSONPaths(json)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) where dynamicType(json.a2) == 'String';
select arrayJoin(distinctJSONPathsAndTypes(json)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) where dynamicType(json.a2) == 'String';
select arrayJoin(distinctDynamicTypes(json.a2)) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) where dynamicType(json.a2) == 'String';

select 'Remote if';
select arrayJoin(distinctJSONPathsIf(json, dynamicType(json.a2) == 'String')) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);
select arrayJoin(distinctJSONPathsAndTypesIf(json, dynamicType(json.a2) == 'String')) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);
select arrayJoin(distinctDynamicTypesIf(json.a2, dynamicType(json.a2) == 'String')) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions);

select 'Remote group by';
select dynamicType(json.a2), distinctJSONPaths(json) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) group by dynamicType(json.a2) order by dynamicType(json.a2);
select dynamicType(json.a2), distinctJSONPathsAndTypes(json) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) group by dynamicType(json.a2) order by dynamicType(json.a2);
select dynamicType(json.a2), distinctDynamicTypes(json.a2) from remote('127.0.0.{1,2,3}', currentDatabase(), test_json_dynamic_aggregate_functions) group by dynamicType(json.a2) order by dynamicType(json.a2);

select distinctJSONPaths() from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctJSONPaths(json, 42) from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctJSONPaths(42) from test_json_dynamic_aggregate_functions; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select distinctJSONPathsAndTypes() from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctJSONPathsAndTypes(json, 42) from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctJSONPathsAndTypes(42) from test_json_dynamic_aggregate_functions; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select distinctDynamicTypes() from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctDynamicTypes(json.a2, 42) from test_json_dynamic_aggregate_functions; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select distinctDynamicTypes(42) from test_json_dynamic_aggregate_functions; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

drop table test_json_dynamic_aggregate_functions;
