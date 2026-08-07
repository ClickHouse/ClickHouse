-- Tags: no-fasttest

set enable_analyzer = 1;
set allow_experimental_json_type = 1;

-- Case 1: Null whole JSON object via VALUES format
-- Triggers ObjectJSONNode::insertResultToColumn null path (JSONExtractTree.cpp)
drop table if exists test_enum_null_obj;
create table test_enum_null_obj (json JSON(e Enum('a' = 1, 'b' = 2))) engine = MergeTree order by tuple();
insert into test_enum_null_obj values ('{"e" : "a"}'), ('{"e" : "b"}'), ('{"e" : null}'), ('{}'), ('null');
select json.e from test_enum_null_obj order by json.e;
drop table test_enum_null_obj;

-- Case 2: Missing JSON column in JSONEachRow format
-- Triggers DataTypeObject::insertDefaultInto via JSONEachRowRowInputFormat
drop table if exists test_enum_missing_col;
create table test_enum_missing_col (x UInt64, json JSON(e Enum('a' = 1, 'b' = 2))) engine = MergeTree order by tuple();
insert into test_enum_missing_col format JSONEachRow {"x": 1}
;

select json.e from test_enum_missing_col;
drop table test_enum_missing_col;

-- Case 3: LEFT JOIN with unmatched rows
-- Triggers DataTypeObject::insertDefaultInto via hash join for unmatched rows
drop table if exists test_enum_join_left;
drop table if exists test_enum_join_right;
create table test_enum_join_left (x UInt64) engine = MergeTree order by tuple();
create table test_enum_join_right (x UInt64, json JSON(e Enum('a' = 1, 'b' = 2))) engine = MergeTree order by tuple();
insert into test_enum_join_left values (1), (2);
insert into test_enum_join_right values (1, '{"e" : "a"}');
select test_enum_join_right.json.e from test_enum_join_left left join test_enum_join_right on test_enum_join_left.x = test_enum_join_right.x order by test_enum_join_left.x;
drop table test_enum_join_left;
drop table test_enum_join_right;
