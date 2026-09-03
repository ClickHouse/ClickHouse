drop table if exists proj;

create table proj (
    bool_value UInt8,
    zero_integer_value Int32,
    integer_value Int32,
    float_value Float32,
    datetime_value DateTime,
    string_value String,
    projection test_projection (
      select
        toStartOfDay (toDateTime (datetime_value)) as Day,
        datetime_value,
        float_value,
        count(
          distinct if(zero_integer_value = 1, string_value, NULL)
        )
      group by
        Day,
        datetime_value,
        float_value
    )
  ) engine MergeTree
partition by
  toDate (datetime_value)
order by
  bool_value;

insert into proj values (1, 1, 1, 1, '2012-10-24 21:30:00', 'ab');

drop table proj;
