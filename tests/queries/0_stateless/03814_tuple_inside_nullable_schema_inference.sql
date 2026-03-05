-- { echo }

SET allow_experimental_nullable_tuple_type=0;

SELECT * FROM format(JSON, '{"x":{"y":1,"z":2}}, {}');

SELECT * FROM format(JSONEachRow, '{"x":{"y":1,"z":2}}, {}');

SET allow_experimental_nullable_tuple_type=1;

SELECT * FROM format(JSON, '{"x":{"y":1,"z":2}}, {}');

SELECT * FROM format(JSONEachRow, '{"x":{"y":1,"z":2}}, {}');
