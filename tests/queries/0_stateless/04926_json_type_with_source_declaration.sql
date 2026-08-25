-- Declaration of the JSON type with the `with_source` parameter.

SELECT toTypeName('{"a" : 42}'::JSON(with_source=1));
SELECT toTypeName('{"a" : 42}'::JSON(with_source=0));
SELECT toTypeName('{"a" : 42}'::JSON(max_dynamic_types=8, with_source=1, max_dynamic_paths=16, a UInt32, SKIP b, SKIP REGEXP 'c.*'));

DROP TABLE IF EXISTS t_json_with_source_declaration;
CREATE TABLE t_json_with_source_declaration (json JSON(with_source=1, a UInt32)) ENGINE = Memory;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_json_with_source_declaration';
DROP TABLE t_json_with_source_declaration;

-- Nested and sub-object types don't have the source of their own.
SELECT toTypeName(json.^a) FROM (SELECT materialize('{"a" : {"b" : 42}}')::JSON(with_source=1) AS json);

-- `__source` is reserved.
SELECT '{}'::JSON(with_source=1, __source UInt32); -- { serverError BAD_ARGUMENTS }
SELECT '{}'::JSON(with_source=1, SKIP __source); -- { serverError BAD_ARGUMENTS }

-- The parameter accepts only 0 and 1.
SELECT '{}'::JSON(with_source=2); -- { serverError UNEXPECTED_AST_STRUCTURE }
SELECT '{}'::JSON(with_source='1'); -- { clientError SYNTAX_ERROR }
SELECT '{}'::JSON(with_source); -- { clientError SYNTAX_ERROR }
SELECT '{}'::JSON(with_sources=1); -- { serverError UNEXPECTED_AST_STRUCTURE }
