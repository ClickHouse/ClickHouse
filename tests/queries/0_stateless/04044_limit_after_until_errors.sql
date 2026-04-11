SET allow_experimental_limit_after = 1;

-- Feature disabled: new analyzer path must reject the query.
SELECT number FROM numbers(5) LIMIT AFTER number >= 2 SETTINGS allow_experimental_limit_after = 0; -- {serverError SUPPORT_IS_DISABLED}

-- Feature disabled: legacy interpreter path must reject the query.
SELECT number FROM numbers(5) LIMIT AFTER number >= 2 SETTINGS enable_analyzer = 0, allow_experimental_limit_after = 0; -- {serverError SUPPORT_IS_DISABLED}

-- Feature disabled: UNTIL-only form (new analyzer).
SELECT number FROM numbers(5) LIMIT UNTIL number >= 2 SETTINGS allow_experimental_limit_after = 0; -- {serverError SUPPORT_IS_DISABLED}

-- Feature disabled: UNTIL-only form (legacy interpreter).
SELECT number FROM numbers(5) LIMIT UNTIL number >= 2 SETTINGS enable_analyzer = 0, allow_experimental_limit_after = 0; -- {serverError SUPPORT_IS_DISABLED}

-- LIMIT WITH TIES is not supported together with AFTER (new analyzer).
SELECT number FROM numbers(5) ORDER BY number LIMIT 3 WITH TIES AFTER number >= 2; -- {serverError NOT_IMPLEMENTED}

-- LIMIT WITH TIES is not supported together with AFTER (legacy interpreter).
SELECT number FROM numbers(5) ORDER BY number LIMIT 3 WITH TIES AFTER number >= 2 SETTINGS enable_analyzer = 0; -- {serverError NOT_IMPLEMENTED}

-- Non-boolean AFTER condition: String cannot be used in boolean context (new analyzer).
SELECT number FROM numbers(5) LIMIT AFTER toString(number); -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}

-- Non-boolean AFTER condition (legacy interpreter).
SELECT number FROM numbers(5) LIMIT AFTER toString(number) SETTINGS enable_analyzer = 0; -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}

-- Non-boolean UNTIL condition: String (new analyzer).
SELECT number FROM numbers(5) LIMIT UNTIL toString(number); -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}

-- Non-boolean UNTIL condition (legacy interpreter).
SELECT number FROM numbers(5) LIMIT UNTIL toString(number) SETTINGS enable_analyzer = 0; -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}
