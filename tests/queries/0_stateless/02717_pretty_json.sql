set allow_experimental_object_type=1;
select 42 as num, [42, 42] as arr, [[[42, 42], [42, 42]], [[42, 42]]] as nested_arr, tuple(42, 42)::Tuple(a UInt32, b UInt32) as tuple, tuple(tuple(tuple(42, 42), 42), 42)::Tuple(a Tuple(b Tuple(c UInt32, d UInt32), e UInt32), f UInt32) as nested_tuple, map(42, 42, 24, 24) as map, map(42, map(42, map(42, 42))) as nested_map, [tuple(map(42, 42), [42, 42]), tuple(map(42, 42), [42, 42])]::Array(Tuple(Map(UInt32, UInt32), Array(UInt32))) as nested_types, '{"a" : {"b" : 1, "c" : 2}}'::JSON as json_object format PrettyNDJSON;

