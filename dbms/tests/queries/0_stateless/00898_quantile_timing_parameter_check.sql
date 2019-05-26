SELECT quantileTiming(0.5)(number) FROM numbers(10);
SELECT quantileTiming(0.5)(number / 2) FROM numbers(10); -- { serverError 43 }
