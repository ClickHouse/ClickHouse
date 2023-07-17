SELECT * FROM numbers(SETTINGS x = 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM numbers(numbers(SETTINGS x = 1)); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM numbers(numbers(SETTINGS x = 1), SETTINGS x = 1); -- { serverError UNKNOWN_FUNCTION }
