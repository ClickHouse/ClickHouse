-- concatWithSeparator and format had the same bug as concat (fixed in #97654):
-- convertToFullIfNeeded recursively stripped LowCardinality from inside a Variant column
-- while the type was not updated, creating a type/column mismatch in serialization.

SET allow_suspicious_low_cardinality_types = 1;

SELECT concatWithSeparator(',', map(number, tuple(number, toLowCardinality(toUInt128(1))), tuple(4), 5), 'x', 'y') FROM numbers(3);
SELECT concatWithSeparator(',', [(number, 2), toLowCardinality(3)], 'x', 'y') FROM numbers(3);

SELECT format('{} {} {}', map(number, tuple(number, toLowCardinality(toUInt128(1))), tuple(4), 5), 'x', 'y') FROM numbers(3);
SELECT format('{} {} {}', [(number, 2), toLowCardinality(3)], 'x', 'y') FROM numbers(3);
