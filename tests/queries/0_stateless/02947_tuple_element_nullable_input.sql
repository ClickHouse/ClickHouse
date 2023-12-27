with 
    cast(tuple(number, number::String) as Tuple(a UInt64, b String)) as t, 
    cast(array(t) as Array(Tuple(a UInt64, b String))) as a,
    cast(array(a) as Array(Array(Tuple(a UInt64, b String)))) as aa
select number, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(a, 'a'), tupleElement(a, 'b'), tupleElement(aa, 'a'), tupleElement(aa, 'b') from numbers(10);

with 
    cast(tuple(number, number::String) as Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))) as t, 
    cast(array(t) as Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))) as a,
    cast(array(a) as Nullable(Array(Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))))) as aa
select number, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(a, 'a'), tupleElement(a, 'b'), tupleElement(aa, 'a'), tupleElement(aa, 'b') from numbers(10);

with
    if(number % 4 = 0, 
    cast(null as Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))), cast(tuple(number, number::String) as Nullable(Tuple(a Nullable(UInt64), b Nullable(String))))) as t,
    cast(array(t) as Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))) as a,
    cast(array(a) as Nullable(Array(Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))))) as aa
select number, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(a, 'a'), tupleElement(a, 'b'), tupleElement(aa, 'a'), tupleElement(aa, 'b') from numbers(10);

with
    if (number % 3 = 0, null, number) as x, if(number % 4 = 0, cast(null as Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))), cast(tuple(x, x::Nullable(String)) as Nullable(Tuple(a Nullable(UInt64), b Nullable(String))))) as t,
    cast(array(t) as Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))) as a,
    cast(array(a) as Nullable(Array(Nullable(Array(Nullable(Tuple(a Nullable(UInt64), b Nullable(String)))))))) as aa
select number, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(a, 'a'), tupleElement(a, 'b'), tupleElement(aa, 'a'), tupleElement(aa, 'b') from numbers(10);

select array(tuple('ClickHouse'))::Array(Tuple(a Nullable(String))) as x, tupleElement(x, 'a');
select array(tuple(null))::Array(Tuple(a Nullable(String))) as x, tupleElement(x, 'a');
select array(null::Nullable(Tuple(a String))) as x, tupleElement(x, 'a');
select null::Nullable(Array(Tuple(a String))) as x,  tupleElement(x, 'a');
