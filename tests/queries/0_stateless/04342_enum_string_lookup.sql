-- Exercises the EnumValues name<->value lookups behind String->Enum casts.
-- Guards the lookup correctness restored together with the O(1) name->value map.

-- Exact String->Enum name lookup, Enum8 and Enum16, including boundary values.
SELECT 'LI'::Enum8('LI' = -128, 'OTHER' = 0, 'LY' = 127);
SELECT 'OTHER'::Enum8('LI' = -128, 'OTHER' = 0, 'LY' = 127);
SELECT 'LY'::Enum8('LI' = -128, 'OTHER' = 0, 'LY' = 127);
SELECT 'min'::Enum16('min' = -32768, 'zero' = 0, 'max' = 32767);
SELECT 'max'::Enum16('min' = -32768, 'zero' = 0, 'max' = 32767);

-- Numeric-string fallback: a string that is not a name is parsed as the underlying value.
SELECT '127'::Enum8('LI' = -128, 'LY' = 127);
SELECT '-128'::Enum8('LI' = -128, 'LY' = 127);

-- A name that looks numeric must be matched as a name, not parsed as a value.
SELECT '1'::Enum8('1' = 42, '2' = 7);

-- The lookup chokepoint is reached through wrapper types as well.
SELECT CAST(materialize('LY'), 'Enum8(\'LI\' = -128, \'LY\' = 127)');
SELECT CAST(toLowCardinality('LY'), 'Enum8(\'LI\' = -128, \'LY\' = 127)');
SELECT CAST(CAST('LY', 'Nullable(String)'), 'Enum8(\'LI\' = -128, \'LY\' = 127)');
SELECT CAST(CAST(NULL, 'Nullable(String)'), 'Nullable(Enum8(\'LI\' = -128, \'LY\' = 127))');

-- Whole-column String->Enum cast (the path measured by the `enum` performance test).
SELECT count(), countDistinct(e) FROM (
    SELECT (number % 3 = 0 ? 'LI' : (number % 3 = 1 ? 'OTHER' : 'LY'))::Enum8('LI' = -128, 'OTHER' = 0, 'LY' = 127) AS e
    FROM numbers(1000)
);

-- Unknown name fails, with a hint.
SELECT 'ZZ'::Enum8('LI' = -128, 'LY' = 127); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- Duplicate names / values are rejected at type construction time.
SELECT CAST('a', 'Enum8(\'a\' = 1, \'a\' = 2)'); -- { serverError SYNTAX_ERROR }
SELECT CAST('a', 'Enum8(\'a\' = 1, \'b\' = 1)'); -- { serverError SYNTAX_ERROR }

-- containsAll: an extended enum contains the narrower one, so the data is preserved on ALTER.
DROP TABLE IF EXISTS t_04342;
CREATE TABLE t_04342 (x Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04342 VALUES ('a'), ('b'), ('a');
ALTER TABLE t_04342 MODIFY COLUMN x Enum8('a' = 1, 'b' = 2, 'c' = 3);
INSERT INTO t_04342 VALUES ('c');
SELECT x, count() FROM t_04342 GROUP BY x ORDER BY x;
DROP TABLE t_04342;
