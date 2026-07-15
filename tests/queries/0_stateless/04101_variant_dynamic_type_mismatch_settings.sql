-- Tests for variant_throw_on_type_mismatch and dynamic_throw_on_type_mismatch settings.
-- These settings control what happens when a function is applied to a Variant/Dynamic column
-- and the actual type of a row is incompatible with the function.
-- Default (true): throw an exception on mismatch.
-- When set to false: return NULL for incompatible rows instead.

SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_type_mismatch;
CREATE TABLE t_type_mismatch
(
    id UInt32,
    v Variant(String, UInt64),
    d Dynamic
) ENGINE = Memory;

INSERT INTO t_type_mismatch VALUES (1, 'hello', 'world'), (2, 42, 123), (3, 'foo', 456);

-- Default behavior (variant_throw_on_type_mismatch = true): throw on incompatible type.
-- length() works on String but not on UInt64.
SELECT length(v) FROM t_type_mismatch ORDER BY id; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Default behavior (dynamic_throw_on_type_mismatch = true): same for Dynamic.
SELECT length(d) FROM t_type_mismatch ORDER BY id; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- With variant_throw_on_type_mismatch = false: return NULL for incompatible rows, keep compatible rows intact.
SET variant_throw_on_type_mismatch = false;
SELECT id, length(v) FROM t_type_mismatch ORDER BY id;

-- With dynamic_throw_on_type_mismatch = false: same for Dynamic.
SET dynamic_throw_on_type_mismatch = false;
SELECT id, length(d) FROM t_type_mismatch ORDER BY id;

DROP TABLE t_type_mismatch;
