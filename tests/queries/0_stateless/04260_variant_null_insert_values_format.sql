SET allow_experimental_variant_type = 1;
SET input_format_null_as_default = 0;

DROP TABLE IF EXISTS test_variant_null_expr;
CREATE TABLE test_variant_null_expr (id Int32, v Variant(String, Int32)) ENGINE = Memory;

INSERT INTO test_variant_null_expr VALUES (1, CAST(NULL AS Nullable(UInt8)));
INSERT INTO test_variant_null_expr VALUES (2, NULL);
INSERT INTO test_variant_null_expr VALUES (3, 'hello');
INSERT INTO test_variant_null_expr VALUES (4, 42);

SELECT id, v, variantType(v) FROM test_variant_null_expr ORDER BY id;

DROP TABLE test_variant_null_expr;
