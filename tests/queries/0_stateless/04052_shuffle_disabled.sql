SELECT number
FROM numbers(10)
SHUFFLE;
-- { serverError SUPPORT_IS_DISABLED }
