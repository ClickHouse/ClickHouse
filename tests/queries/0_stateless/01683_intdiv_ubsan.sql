SELECT DISTINCT intDiv(number, nan) FROM numbers(10); -- { serverError 153 }
