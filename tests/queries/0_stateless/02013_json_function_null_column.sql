-- Tags: no-fasttest

SELECT JSONExtract('{"string_value":null}', 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract('{"string_value":null}', 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'String') as x, toTypeName(x);
