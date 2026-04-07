SELECT dictGetString(concat('default', '.countryId'), 'country', toUInt64(number)) AS country FROM numbers(2) GROUP BY country; -- { serverError BAD_ARGUMENTS }
