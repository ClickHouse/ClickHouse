SET allow_experimental_variant_type = 1;
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) as type FROM test;
SELECT toTypeName(variantType(v)) from test limit 1;

SELECT variantType() FROM test; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT variantType(v, v) FROM test; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT variantType(v.String) FROM test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT variantType(v::Variant(UInt64, String, Array(UInt64), Date)) as type FROM test;
SELECT toTypeName(variantType(v::Variant(UInt64, String, Array(UInt64), Date))) from test limit 1;

