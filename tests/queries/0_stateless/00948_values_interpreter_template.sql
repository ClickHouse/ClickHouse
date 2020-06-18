DROP TABLE IF EXISTS type_names;
DROP TABLE IF EXISTS values_template;
DROP TABLE IF EXISTS values_template_nullable;
DROP TABLE IF EXISTS values_template_fallback;

CREATE TABLE type_names (n UInt8, s1 String, s2 String, s3 String) ENGINE=Memory;
CREATE TABLE values_template (d Date, s String, u UInt8, i Int64, f Float64, a Array(UInt8)) ENGINE = Memory;
CREATE TABLE values_template_nullable (d Date, s Nullable(String), u Nullable(UInt8), a Array(Nullable(Float32))) ENGINE = Memory;
CREATE TABLE values_template_fallback (n UInt8) ENGINE = Memory;

SET input_format_values_interpret_expressions = 0;

-- checks type deduction
INSERT INTO type_names VALUES (1, toTypeName([1, 2]), toTypeName((256, -1, 3.14, 'str', [1, -1])), toTypeName([(1, [256]), (256, [1, 2])])), (2, toTypeName([1, -1]), toTypeName((256, -1, 3, 'str', [1, 2])), toTypeName([(256, []), (1, [])]));

--(1, lower(replaceAll(_STR_1, 'o', 'a')), _NUM_1 + _NUM_2 + _NUM_3, round(_NUM_4 / _NUM_5), _NUM_6 * CAST(_STR_7, 'Int8'), _ARR_8);
-- _NUM_1: UInt64 -> Int64 -> UInt64
-- _NUM_4: Int64 -> UInt64
-- _NUM_5: Float64 -> Int64
INSERT INTO values_template VALUES ((1), lower(replaceAll('Hella', 'a', 'o')), 1 + 2 + 3, round(-4 * 5.0), nan / CAST('42', 'Int8'), reverse([1, 2, 3])), ((2), lower(replaceAll('Warld', 'a', 'o')), -4 + 5 + 6, round(18446744073709551615 * 1e-19), 1.0 / CAST('0', 'Int8'), reverse([])), ((3), lower(replaceAll('Test', 'a', 'o')), 3 + 2 + 1, round(9223372036854775807 * -1), 6.28  / CAST('2', 'Int8'), reverse([4, 5])), ((4), lower(replaceAll('Expressians', 'a', 'o')), 6 + 5 + 4, round(1 * -9223372036854775807), 127.0 / CAST('127', 'Int8'), reverse([6, 7, 8, 9, 0]));

INSERT INTO values_template_nullable VALUES ((1), lower(replaceAll('Hella', 'a', 'o')), 1 + 2 + 3, arraySort(x -> assumeNotNull(x), [null, NULL])),   ((2), lower(replaceAll('Warld', 'b', 'o')), 4 - 5 + 6, arraySort(x -> assumeNotNull(x), [+1, -1, Null])),   ((3), lower(replaceAll('Test', 'c', 'o')), 3 + 2 - 1, arraySort(x -> assumeNotNull(x), [1, nUlL, 3.14])),   ((4), lower(replaceAll(null, 'c', 'o')), 6 + 5 - null, arraySort(x -> assumeNotNull(x), [3, 2, 1]));

INSERT INTO values_template_fallback VALUES (1 + x); -- { clientError 62 }
INSERT INTO values_template_fallback VALUES (abs(functionThatDoesNotExists(42))); -- { clientError 46 }
INSERT INTO values_template_fallback VALUES ([1]); -- { clientError 43 }

INSERT INTO values_template_fallback VALUES (CAST(1, 'UInt8')), (CAST('2', 'UInt8'));
SET input_format_values_accurate_types_of_literals = 0;

INSERT INTO type_names VALUES (3, toTypeName([1, 2]), toTypeName((256, -1, 3.14, 'str', [1, -1])), toTypeName([(1, [256]), (256, [1, 2])])), (4, toTypeName([1, -1]), toTypeName((256, -1, 3, 'str', [1, 2])), toTypeName([(256, []), (1, [])]));
SET input_format_values_interpret_expressions = 1;
INSERT INTO values_template_fallback VALUES (1 + 2), (3 + +04), (5 + 6);
INSERT INTO values_template_fallback VALUES (+020), (+030), (+040);

SELECT * FROM type_names ORDER BY n;
SELECT * FROM values_template ORDER BY d;
SELECT * FROM values_template_nullable ORDER BY d;
SELECT * FROM values_template_fallback ORDER BY n;
DROP TABLE type_names;
DROP TABLE values_template;
DROP TABLE values_template_nullable;
DROP TABLE values_template_fallback;
