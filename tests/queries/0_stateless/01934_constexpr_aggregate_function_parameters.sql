SELECT groupArray(2 + 3)(number) FROM numbers(10);
SELECT groupArray('5'::UInt8)(number) FROM numbers(10);

SELECT groupArray(NULL)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(NULL + NULL)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupArray([])(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(throwIf(1))(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS, 134 }

-- Not the best error message, can be improved.
SELECT groupArray(number)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS, 47 }
