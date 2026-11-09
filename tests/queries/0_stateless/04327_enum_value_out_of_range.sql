-- Out-of-range integer values in Enum8/Enum16 type definitions must be rejected
-- with ARGUMENT_OUT_OF_BOUND instead of being silently truncated by a narrowing cast.

SELECT CAST('a', 'Enum8(\'a\' = 200)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum8(\'a\' = -200)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum16(\'a\' = 40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum16(\'a\' = -40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Auto-detected Enum promotes to Enum16, but a value out of Int16 range still throws.
SELECT CAST('a', 'Enum(\'a\' = 40000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum(\'a\' = 100000)'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Values above Int64 (read as UInt64 during parsing) must not wrap around to a small in-range
-- value: 18446744073709551615 would become -1 under a plain cast and be silently accepted.
SELECT CAST('a', 'Enum8(\'a\' = 18446744073709551615)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum16(\'a\' = 18446744073709551615)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum(\'a\' = 18446744073709551615)'); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- The generic parser path (forced here by the trailing auto-assigned element) has the same wrap.
SELECT CAST('a', 'Enum8(\'z\' = 18446744073709551615, \'a\')'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The CREATE TABLE path uses the same factory and must reject it too.
CREATE TABLE t_04327 (x Enum8('a' = 200)) ENGINE = Memory; -- { serverError ARGUMENT_OUT_OF_BOUND }
CREATE TABLE t_04327 (x Enum8('a' = 18446744073709551615)) ENGINE = Memory; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Auto-assignment continues from the previous element. A negative first value must keep its
-- sign: 'a' = -1000 makes 'b' = -999 (not a wrapped-around huge UInt64). This is the case that
-- broke after the above-Int64 guard was added (see 00757_enum_defaults).
SELECT toInt16(CAST('b', 'Enum(\'a\' = -1000, \'b\')'));
SELECT toInt8(CAST('c', 'Enum8(\'a\' = -5, \'b\', \'c\')'));

-- A first value at Int64 max followed by an auto-assigned element overflows Int64 when the next
-- number is synthesized; it must be rejected cleanly, not wrapped, before the range check.
SELECT CAST('a', 'Enum8(\'z\' = 9223372036854775807, \'a\')'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST('a', 'Enum(\'z\' = 9223372036854775807, \'a\')'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Boundary values are valid.
SELECT toInt8(CAST('a', 'Enum8(\'a\' = 127, \'b\' = -128)'));
SELECT toInt16(CAST('a', 'Enum16(\'a\' = 32767, \'b\' = -32768)'));

-- Auto-detection still promotes to Enum16 for values outside Int8 but inside Int16.
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 200, \'b\' = -200)'));

-- DataTypeFactory::tryGet must keep returning nullptr (not throw) on invalid enum type text.
-- Dynamic subcolumn detection parses the subcolumn name as a type via tryGet and uses the null
-- result as the "not a subcolumn" signal; an out-of-range enum value must therefore surface as
-- UNKNOWN_IDENTIFIER (subcolumn not found), not leak ARGUMENT_OUT_OF_BOUND from the type parse.
-- enable_analyzer=1: Dynamic-subcolumn access by type-name text is an analyzer feature; the
-- old analyzer rejects the identifier before tryGet runs, so pin it on these two cases only.
SELECT d.`Enum8('a' = 200)` FROM (SELECT CAST('42', 'Dynamic') AS d) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_IDENTIFIER }
-- A valid enum subcolumn name is still parsed fine (the subcolumn simply isn't present -> NULL).
SELECT d.`Enum8('a' = 100)` FROM (SELECT CAST('42', 'Dynamic') AS d) SETTINGS enable_analyzer = 1;
