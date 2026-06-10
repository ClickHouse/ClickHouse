-- Out-of-range integer values in Enum8/Enum16 type definitions must be rejected
-- with ARGUMENT_OUT_OF_BOUND instead of being silently truncated by a narrowing cast.

SELECT CAST('a', 'Enum8(\'a\' = 200)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum8(\'a\' = -200)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum16(\'a\' = 40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum16(\'a\' = -40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Auto-detected Enum promotes to Enum16, but a value out of Int16 range still throws.
SELECT CAST('a', 'Enum(\'a\' = 40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum(\'a\' = 100000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The CREATE TABLE path uses the same factory and must reject it too.
CREATE TABLE t_04327 (x Enum8('a' = 200)) ENGINE = Memory; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Boundary values are valid.
SELECT toInt8(CAST('a', 'Enum8(\'a\' = 127, \'b\' = -128)'));
SELECT toInt16(CAST('a', 'Enum16(\'a\' = 32767, \'b\' = -32768)'));

-- Auto-detection still promotes to Enum16 for values outside Int8 but inside Int16.
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 200, \'b\' = -200)'));
