-- Tags: no-fasttest, no-parallel

set engine_file_truncate_on_insert = 1;
set flatten_nested = 0;

insert into function file('02906.orc', 'ORC')
select
    number::Int64 as int64_column,
    number::String as string_column,
    number::Float64 as float64_column,
    cast(if(number % 10 = 0, tuple(null, null, null), tuple(number::String, number::Float64, number::Int64)) as Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64))) as tuple_column,
    cast(if(number % 10 = 0, array(tuple(null, null, null)), array(tuple(number::String, number::Float64, number::Int64))) as Array(Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64)))) as array_tuple_column,
    cast(if(number % 10 = 0, map(number::String, tuple(null, null, null)), map(number::String, tuple(number::String, number::Float64, number::Int64))) as Map(String, Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64)))) as map_tuple_column
    from numbers(100);

desc file('02906.orc');

-- { echoOn }
-- Test primitive types
select int64_column, string_column, float64_column from file('02906.orc') where int64_column % 15 = 0;

-- Test tuple type with names
select tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, tuple_column Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64))') where int64_column % 15 = 0;
select tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, tuple_column Tuple(c Nullable(Int64))') where int64_column % 15 = 0;
select tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, tuple_column Tuple(c Nullable(Int64), d Nullable(String))') where int64_column % 15 = 0;

-- Test tuple type without names
select tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, tuple_column Tuple(Nullable(String), Nullable(Float64), Nullable(Int64))') where int64_column % 15 = 0;
select tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, tuple_column Tuple(Nullable(String), Nullable(Float64))') where int64_column % 15 = 0;

-- Test tuple nested in array
select array_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, array_tuple_column Array(Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64)))') where int64_column % 15 = 0;
select array_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, array_tuple_column Array(Tuple(b Nullable(Float64), c Nullable(Int64)))') where int64_column % 15 = 0;
select array_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, array_tuple_column Array(Tuple(b Nullable(Float64), c Nullable(Int64), d Nullable(String)))') where int64_column % 15 = 0;

-- Test tuple nested in map
select map_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, map_tuple_column Map(String, Tuple(a Nullable(String), b Nullable(Float64), c Nullable(Int64)))') where int64_column % 15 = 0;
select map_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, map_tuple_column Map(String, Tuple(b Nullable(Float64), c Nullable(Int64)))') where int64_column % 15 = 0;
select map_tuple_column from file('02906.orc', 'ORC', 'int64_column Int64, map_tuple_column Map(String, Tuple(b Nullable(Float64), c Nullable(Int64), d Nullable(String)))') where int64_column % 15 = 0;
-- { echoOff }
