-- `STRING_AGG` is a PostgreSQL/SQL-standard alias of `groupConcat`. Argument
-- order matches: `STRING_AGG(expr, separator)`.
-- The 2-argument form is only supported by the new analyzer (same as
-- `groupConcat(expr, sep)`; see `03156_group_concat.sql`).
SELECT STRING_AGG(toString(number), ',') FROM numbers(5) SETTINGS enable_analyzer=1;
SELECT string_agg(toString(number), '-') FROM numbers(3) SETTINGS enable_analyzer=1;
SELECT STRING_AGG(toString(number)) FROM numbers(3);
