SELECT uniqUpTo(1e100)(number) FROM numbers(5); -- { serverError 70 }
SELECT uniqUpTo(-1e100)(number) FROM numbers(5); -- { serverError 70 }
