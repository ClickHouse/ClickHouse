SET input_format_try_infer_variants=1;
SELECT arr, toTypeName(arr) FROM format('JSONEachRow', '{"arr" : [1, "Hello", {"a" : 32}]}') FORMAT Pretty;
SELECT x, toTypeName(x) FROM format('JSONEachRow', '{"x" : 42}, {"x" : "Hello"}') FORMAT Pretty;
SELECT x, toTypeName(x) FROM format('JSONEachRow', '{"x" : [1, 2, 3]}, {"x" : {"a" : 42}}') FORMAT Pretty;
SELECT c1, toTypeName(c1), c2, toTypeName(c2) FROM format('CSV', '1,Hello World!\n2,"[1,2,3]"\n3,"2020-01-01"\n') FORMAT Pretty;