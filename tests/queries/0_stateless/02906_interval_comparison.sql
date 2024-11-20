-- Comparing the same types is ok:
SELECT INTERVAL 1 SECOND = INTERVAL 1 SECOND;
-- It is reasonable to not give an answer for this:
SELECT INTERVAL 30 DAY < INTERVAL 1 MONTH; -- { serverError 386 }
-- This we could change in the future:
SELECT INTERVAL 1 SECOND = INTERVAL 1 YEAR; -- { serverError 386 }
SELECT INTERVAL 1 SECOND <= INTERVAL 1 YEAR; -- { serverError 386 }
