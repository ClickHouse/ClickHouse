-- Test Array empty (size0 substream)
drop table if exists test_empty_array;
create table test_empty_array (id UInt64, `a.size0` UInt64, a Array(UInt64)) engine=MergeTree order by tuple();
insert into test_empty_array select 42, 42, [];
-- { echo }
select id from test_empty_array where empty(a) settings optimize_functions_to_subcolumns=1;
select id from test_empty_array where empty(a) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Array notEmpty (size0 substream)
drop table if exists test_notempty_array;
create table test_notempty_array (id UInt64, `a.size0` UInt64, a Array(UInt64)) engine=MergeTree order by tuple();
insert into test_notempty_array select 42, 42, [1];
-- { echo }
select id from test_notempty_array where notEmpty(a) settings optimize_functions_to_subcolumns=1;
select id from test_notempty_array where notEmpty(a) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Array length (size0 substream)
drop table if exists test_length_array;
create table test_length_array (id UInt64, `a.size0` UInt64, a Array(UInt64)) engine=MergeTree order by tuple();
insert into test_length_array select 42, 100, [1, 2, 3];
-- { echo }
select id, length(a) from test_length_array settings optimize_functions_to_subcolumns=1;
select id, length(a) from test_length_array settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test String empty (size substream)
drop table if exists test_empty_string;
create table test_empty_string (id UInt64, `s.size` UInt64, s String) engine=MergeTree order by tuple();
insert into test_empty_string select 42, 42, '';
-- { echo }
select id from test_empty_string where empty(s) settings optimize_functions_to_subcolumns=1;
select id from test_empty_string where empty(s) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test String notEmpty (size substream)
drop table if exists test_notempty_string;
create table test_notempty_string (id UInt64, `s.size` UInt64, s String) engine=MergeTree order by tuple();
insert into test_notempty_string select 42, 42, 'hello';
-- { echo }
select id from test_notempty_string where notEmpty(s) settings optimize_functions_to_subcolumns=1;
select id from test_notempty_string where notEmpty(s) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test String length (size substream)
drop table if exists test_length_string;
create table test_length_string (id UInt64, `s.size` UInt64, s String) engine=MergeTree order by tuple();
insert into test_length_string select 42, 100, 'hello';
-- { echo }
select id, length(s) from test_length_string settings optimize_functions_to_subcolumns=1;
select id, length(s) from test_length_string settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Map empty (size0 substream)
drop table if exists test_empty_map;
create table test_empty_map (id UInt64, `m.size0` UInt64, m Map(String, UInt64)) engine=MergeTree order by tuple();
insert into test_empty_map select 42, 42, map();
-- { echo }
select id from test_empty_map where empty(m) settings optimize_functions_to_subcolumns=1;
select id from test_empty_map where empty(m) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Map notEmpty (size0 substream)
drop table if exists test_notempty_map;
create table test_notempty_map (id UInt64, `m.size0` UInt64, m Map(String, UInt64)) engine=MergeTree order by tuple();
insert into test_notempty_map select 42, 42, map('a', 1);
-- { echo }
select id from test_notempty_map where notEmpty(m) settings optimize_functions_to_subcolumns=1;
select id from test_notempty_map where notEmpty(m) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Map length (size0 substream)
drop table if exists test_length_map;
create table test_length_map (id UInt64, `m.size0` UInt64, m Map(String, UInt64)) engine=MergeTree order by tuple();
insert into test_length_map select 42, 100, map('a', 1, 'b', 2);
-- { echo }
select id, length(m) from test_length_map settings optimize_functions_to_subcolumns=1;
select id, length(m) from test_length_map settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Map mapKeys (keys substream)
drop table if exists test_mapkeys;
create table test_mapkeys (id UInt64, `m.keys` Array(String), m Map(String, UInt64)) engine=MergeTree order by tuple(); -- { serverError BAD_ARGUMENTS }

-- Test Map mapValues (values substream)
drop table if exists test_mapvalues;
create table test_mapvalues (id UInt64, `m.values` Array(UInt64), m Map(String, UInt64)) engine=MergeTree order by tuple(); -- { serverError BAD_ARGUMENTS }

-- Test Map mapContainsKey (keys substream)
drop table if exists test_mapcontainskey;
create table test_mapcontainskey (id UInt64, `m.keys` Array(String), m Map(String, UInt64)) engine=MergeTree order by tuple(); -- { serverError BAD_ARGUMENTS }

-- Test Nullable isNull (null substream)
drop table if exists test_isnull;
create table test_isnull (id UInt64, `n.null` UInt8, n Nullable(UInt64)) engine=MergeTree order by tuple();
insert into test_isnull select 42, 1, NULL;
-- { echo }
select id from test_isnull where isNull(n) settings optimize_functions_to_subcolumns=1;
select id from test_isnull where isNull(n) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Nullable isNotNull (null substream)
drop table if exists test_isnotnull;
create table test_isnotnull (id UInt64, `n.null` UInt8, n Nullable(UInt64)) engine=MergeTree order by tuple();
insert into test_isnotnull select 42, 1, 100;
-- { echo }
select id from test_isnotnull where isNotNull(n) settings optimize_functions_to_subcolumns=1;
select id from test_isnotnull where isNotNull(n) settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Nullable count (null substream)
drop table if exists test_count_nullable;
create table test_count_nullable (id UInt64, `n.null` UInt8, n Nullable(UInt64)) engine=MergeTree order by tuple();
insert into test_count_nullable select 42, 0, 100;
insert into test_count_nullable select 43, 1, NULL;
-- { echo }
select count(n) from test_count_nullable settings optimize_functions_to_subcolumns=1;
select count(n) from test_count_nullable settings optimize_functions_to_subcolumns=0;
-- { echoOff }

-- Test Tuple tupleElement (named subcolumn)
drop table if exists test_tupleelement;
create table test_tupleelement (id UInt64, `t.a` UInt64, t Tuple(a UInt64, b String)) engine=MergeTree order by tuple(); -- { serverError BAD_ARGUMENTS }
