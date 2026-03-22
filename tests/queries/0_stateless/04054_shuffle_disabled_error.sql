SELECT number
FROM numbers(10)
WHERE 1
SHUFFLE; -- { serverError SUPPORT_IS_DISABLED }
