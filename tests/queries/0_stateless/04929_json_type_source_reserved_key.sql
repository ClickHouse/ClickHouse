-- A top level key with the name of the source subcolumn is not allowed in types with `with_source=1`.

SELECT '{"__source" : 42}'::JSON(with_source=1); -- { serverError INCORRECT_DATA }
SELECT json FROM format(JSONEachRow, 'json JSON(with_source=1)', '{"json" : {"a" : 42, "__source" : "x"}}'); -- { serverError INCORRECT_DATA }

-- The same for the paths nested in it, whether the key contains a dot or holds an object.
SELECT '{"__source.a" : 42}'::JSON(with_source=1); -- { serverError INCORRECT_DATA }
SELECT '{"__source" : {"a" : 42}}'::JSON(with_source=1); -- { serverError INCORRECT_DATA }

-- A key that only starts with the name is an ordinary path.
SELECT '{"__sourcex" : 42}'::JSON(with_source=1);

-- Nested keys with this name are fine.
SELECT ('{"a" : {"__source" : 42}}'::JSON(with_source=1)).`a`.`__source`;
SELECT '{"a" : {"__source" : 42}}'::JSON(with_source=1);

-- Without the parameter the key is an ordinary path.
SELECT '{"__source" : 42}'::JSON;
SELECT ('{"__source" : 42}'::JSON).__source;
