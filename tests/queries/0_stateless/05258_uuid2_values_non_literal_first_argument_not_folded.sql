-- `values` treats its first argument as a schema only when it is already a string literal; a constant string
-- expression there is the first row. Materializing `uuid_type_version = 2` into a persisted definition must not
-- fold such an expression into a literal schema, or the stored query would switch to the schema overload.

DROP VIEW IF EXISTS uuid2_values_expression;
DROP VIEW IF EXISTS uuid2_values_literal;

SET uuid_type_version = 2;

-- Two `String` rows, before and after the definition is persisted.
CREATE VIEW uuid2_values_expression AS SELECT * FROM values(concat('id ', 'UUID'), 'not-a-uuid');
SELECT position(create_table_query, 'UUID2') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_values_expression';
SELECT toTypeName(*), * FROM uuid2_values_expression ORDER BY ALL;

-- A literal schema is still materialized.
CREATE VIEW uuid2_values_literal AS SELECT * FROM values('id UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT position(create_table_query, 'UUID2') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_values_literal';
SET uuid_type_version = 1;
SELECT toTypeName(id), id FROM uuid2_values_literal;

DROP VIEW uuid2_values_expression;
DROP VIEW uuid2_values_literal;
