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

-- AFTER/UNTIL boundary over a non-grouped raw column must be rejected by aggregate validation,
-- not crash later in the planner. Aggregate-only query (no GROUP BY).
SELECT count() FROM numbers(10) LIMIT AFTER number > 0; -- {serverError NOT_AN_AGGREGATE}
SELECT count() FROM numbers(10) LIMIT UNTIL number > 0; -- {serverError NOT_AN_AGGREGATE}

-- Grouped query, AFTER/UNTIL references a column that is neither a GROUP BY key nor an aggregate.
SELECT k, count() FROM (SELECT number AS k, number AS v FROM numbers(10)) GROUP BY k LIMIT AFTER v > 0; -- {serverError NOT_AN_AGGREGATE}
SELECT k, count() FROM (SELECT number AS k, number AS v FROM numbers(10)) GROUP BY k LIMIT UNTIL v > 0; -- {serverError NOT_AN_AGGREGATE}
