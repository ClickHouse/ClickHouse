-- Tags: no-fasttest

SELECT dateDiff('minute', ULIDStringToDateTime(generateULID()), now()) <= 1;
SELECT toTimezone(ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9E'), 'America/Costa_Rica');
SELECT ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9E', 'America/Costa_Rica');
SELECT ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9', 'America/Costa_Rica'); -- { serverError ILLEGAL_COLUMN }
SELECT ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9E', 'America/Costa_Ric'); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9E0'); -- { serverError ILLEGAL_COLUMN }
SELECT ULIDStringToDateTime(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ULIDStringToDateTime(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
