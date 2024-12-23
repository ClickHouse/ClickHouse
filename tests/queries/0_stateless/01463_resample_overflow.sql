select groupArrayResample(-9223372036854775808, 9223372036854775807, 9223372036854775807)(number, toInt64(number)) FROM numbers(7); -- { serverError 69 }
