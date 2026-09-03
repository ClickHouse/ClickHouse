SELECT uniqUpTo(1e100)(number) FROM numbers(5); -- { serverError CANNOT_CONVERT_TYPE }
SELECT uniqUpTo(-1e100)(number) FROM numbers(5); -- { serverError CANNOT_CONVERT_TYPE }
