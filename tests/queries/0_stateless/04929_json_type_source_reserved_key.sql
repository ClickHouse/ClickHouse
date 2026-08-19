-- A top level key with the name of the source subcolumn is not allowed in types with `with_source=1`.

SELECT '{"__source" : 42}'::JSON(with_source=1); -- { serverError INCORRECT_DATA }
SELECT json FROM format(JSONEachRow, 'json JSON(with_source=1)', '{"json" : {"a" : 42, "__source" : "x"}}'); -- { serverError INCORRECT_DATA }

-- Nested keys with this name are fine.
SELECT ('{"a" : {"__source" : 42}}'::JSON(with_source=1)).`a`.`__source`;
SELECT '{"a" : {"__source" : 42}}'::JSON(with_source=1);

-- Without the parameter the key is an ordinary path.
SELECT '{"__source" : 42}'::JSON;
SELECT ('{"__source" : 42}'::JSON).__source;
