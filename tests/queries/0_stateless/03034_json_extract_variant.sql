select JSONExtract('{"a" : 42}', 'a', 'Variant(String, UInt32)') as v, variantType(v);
select JSONExtract('{"a" : "Hello"}', 'a', 'Variant(String, UInt32)') as v, variantType(v);
select JSONExtract('{"a" : [1, 2, 3]}', 'a', 'Variant(String, Array(UInt32))') as v, variantType(v);
select JSONExtract('{"obj" : {"a" : 42, "b" : "Hello", "c" : [1,2,3]}}', 'obj', 'Map(String, Variant(UInt32, String, Array(UInt32)))');
select JSONExtractKeysAndValues('{"a" : 42, "b" : "Hello", "c" : [1,2,3]}', 'Variant(UInt32, String, Array(UInt32))') as v, toTypeName(v);

