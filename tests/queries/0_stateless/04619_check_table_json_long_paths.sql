-- Test for issue #100153: `CHECK TABLE` reported healthy parts as broken for `JSON`
-- columns with paths longer than the filesystem name limit, because probing the raw
-- substream file name threw `File name too long` before the hashed name was tried.

set enable_json_type = 1;

drop table if exists t_04619;
create table t_04619 (j JSON) engine = MergeTree order by tuple()
    settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 1;

insert into t_04619 select toJSONString(map(repeat('a', 300), 42));

select 'check result';
check table t_04619 settings check_query_single_value_result = 1;

drop table t_04619;
