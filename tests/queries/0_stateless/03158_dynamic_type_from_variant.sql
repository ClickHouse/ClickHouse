SET allow_experimental_dynamic_type=1;
SET allow_experimental_variant_type=1;
SET allow_suspicious_types_in_order_by=1;

CREATE TABLE test_variable (v Variant(String, UInt32, IPv6, Bool, DateTime64)) ENGINE = Memory;
CREATE TABLE test_dynamic (d Dynamic) ENGINE = Memory;

INSERT INTO test_variable VALUES (1), ('s'), (0), ('0'), ('true'), ('false'), ('2001-01-01 01:01:01.111'), (NULL);

SELECT v, toTypeName(v) FROM test_variable ORDER BY v;

INSERT INTO test_dynamic SELECT * FROM test_variable;

SELECT '';
SELECT d, dynamicType(d) FROM test_dynamic ORDER BY d;
