SET log_queries_probability = inf; --  { serverError CANNOT_PARSE_NUMBER }
SET log_queries_probability = nan; --  { serverError CANNOT_PARSE_NUMBER }
SET log_queries_probability = -inf; --  { serverError CANNOT_PARSE_NUMBER }

SELECT 1 SETTINGS log_queries_probability = -inf; --  { clientError CANNOT_PARSE_NUMBER }
