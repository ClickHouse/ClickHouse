-- Comparing the same types is ok:
SELECT INTERVAL 1 SECOND = INTERVAL 1 SECOND;
-- It is reasonable to not give an answer for this:
SELECT INTERVAL 30 DAY < INTERVAL 1 MONTH; -- { serverError NO_COMMON_TYPE }
-- This we could change in the future:
SELECT INTERVAL 1 SECOND = INTERVAL 1 YEAR; -- { serverError NO_COMMON_TYPE }
SELECT INTERVAL 1 SECOND <= INTERVAL 1 YEAR; -- { serverError NO_COMMON_TYPE }
