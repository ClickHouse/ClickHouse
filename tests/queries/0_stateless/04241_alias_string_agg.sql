-- STRING_AGG is a PostgreSQL/SQL-standard alias of groupConcat. Argument
-- order matches: STRING_AGG(expr, separator).
SELECT STRING_AGG(toString(number), ',') FROM numbers(5);
SELECT string_agg(toString(number), '-') FROM numbers(3);
SELECT STRING_AGG(toString(number)) FROM numbers(3);
