SELECT parseDateTimeBestEffort(toFixedString('01/12/2017,', 11)); -- { serverError CANNOT_PARSE_DATETIME }
