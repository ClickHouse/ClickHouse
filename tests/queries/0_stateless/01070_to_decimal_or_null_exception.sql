SELECT toDecimal32('e', 1); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toDecimal64('e', 2); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toDecimal128('e', 3); -- { serverError CANNOT_PARSE_NUMBER }

SELECT toDecimal32OrNull('e', 1) x, isNull(x);
SELECT toDecimal64OrNull('e', 2) x, isNull(x);
SELECT toDecimal128OrNull('e', 3) x, isNull(x);
