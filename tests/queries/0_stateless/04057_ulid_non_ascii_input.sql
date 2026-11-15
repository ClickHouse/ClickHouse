-- Tags: no-fasttest
-- Test that ULIDStringToDateTime properly rejects non-ASCII input without buffer overflow

SELECT ULIDStringToDateTime(unhex(repeat('ff', 26))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToDateTime(unhex(repeat('80', 26))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToDateTime(unhex(repeat('fe', 26))); -- { serverError BAD_ARGUMENTS }

-- Valid ULID should still work
SELECT ULIDStringToDateTime('01GWJWKW30MFPQJRYEAF4XFZ9E', 'UTC');
