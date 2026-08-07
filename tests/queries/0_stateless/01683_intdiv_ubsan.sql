SELECT DISTINCT intDiv(number, nan) FROM numbers(10); -- { serverError ILLEGAL_DIVISION }
