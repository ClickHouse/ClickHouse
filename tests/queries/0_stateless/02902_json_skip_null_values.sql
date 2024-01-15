-- Tags: no-fasttest

create table test_02902 engine File(JSONEachRow)
    settings output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 1
    as select cast((number::String, null, (number::String, null)), 'Tuple(a Nullable(String), b Nullable(Int64), c Tuple(x Nullable(String), y Nullable(Float64)))') as c
        from numbers(3);

select * from test_02902 format JSONEachRow settings output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 1;
select * from test_02902 format JSONEachRow settings output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 0;

drop table test_02902; 

select
  toJSONString(c)
from
  (
    select
      cast(
        (number:: String, null, (number:: String, null)),
        'Tuple(a Nullable(String), b Nullable(Int64), c Tuple(x Nullable(String), y Nullable(Float64)))'
      ) as c
    from
      numbers(3)
  )
settings output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 0;

select
  toJSONString(c)
from
  (
    select
      cast(
        (number:: String, null, (number:: String, null)),
        'Tuple(a Nullable(String), b Nullable(Int64), c Tuple(x Nullable(String), y Nullable(Float64)))'
      ) as c
    from
      numbers(3)
  )
settings output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 1;
