-- repeat() with this length and this number of rows will allocation huge enough region (MSB set),
-- which will cause roundUpToPowerOfTwoOrZero() returns 0 for such allocation (before the fix),
-- and later repeat() will try to use this memory and will got SIGSEGV.
SELECT repeat('0.0001048576', number * (number * (number * 255))) FROM numbers(65535); -- { serverError 131; }
