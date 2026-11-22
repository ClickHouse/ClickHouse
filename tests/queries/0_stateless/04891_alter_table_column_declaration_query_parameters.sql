DROP TABLE IF EXISTS t_column_decl_param;

CREATE TABLE t_column_decl_param (id UInt64) ENGINE = MergeTree ORDER BY tuple();

-- ADD COLUMN with a query parameter in the column-declaration name position
SET param_add_col = 'added_col';
ALTER TABLE t_column_decl_param ADD COLUMN {add_col:Identifier} String;
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_column_decl_param' AND name = {add_col:String}
FORMAT TSV;

-- MODIFY COLUMN with a query parameter in the column-declaration name position
SET param_mod_col = 'id';
ALTER TABLE t_column_decl_param MODIFY COLUMN {mod_col:Identifier} UInt32;
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_column_decl_param' AND name = {mod_col:String}
FORMAT TSV;

-- ALTER COLUMN with a query parameter in the column-declaration name position
SET param_alter_col = 'added_col';
ALTER TABLE t_column_decl_param ALTER COLUMN {alter_col:Identifier} TYPE UInt32;
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_column_decl_param' AND name = {alter_col:String}
FORMAT TSV;

-- CREATE TABLE with a query parameter in the column-declaration name position
SET param_create_col = 'created_col';
CREATE TABLE t_column_decl_param_create ({create_col:Identifier} UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_column_decl_param_create' AND name = {create_col:String}
FORMAT TSV;
DROP TABLE t_column_decl_param_create;

-- A query parameter of a non-`Identifier` type is rejected in the name position by the client-side parser
ALTER TABLE t_column_decl_param ADD COLUMN {bad:String} UInt64; -- { clientError SYNTAX_ERROR }

-- An unset query parameter in the name position fails when parameters are substituted
ALTER TABLE t_column_decl_param ADD COLUMN {unset:Identifier} UInt64; -- { serverError UNKNOWN_QUERY_PARAMETER }

-- AST JSON round-trip preserves a query parameter in the column-declaration name position
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ({c:Identifier} UInt8) ENGINE = Memory'));

-- Query parameters are not supported in table-function structure strings, which are parsed
-- at execution time without parameter substitution
SELECT * FROM null('{c:Identifier} UInt8'); -- { serverError SYNTAX_ERROR }

DROP TABLE t_column_decl_param;
