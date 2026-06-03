-- AT TIME ZONE postfix operator (SQL standard, PostgreSQL-compatible)
-- expr AT TIME ZONE 'tz'  →  toTimeZone(expr, 'tz')
-- expr AT LOCAL           →  toTimeZone(expr, timeZone())

SET session_timezone = 'UTC';

-- Basic PostgreSQL-compatible syntax
SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';

-- Must produce the same result as the function form
SELECT (TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver') = toTimeZone(TIMESTAMP '2001-02-16 20:38:40', 'America/Denver');

-- AT LOCAL converts to the session timezone (UTC in this test)
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;

-- AT LOCAL must equal toTimeZone(expr, timeZone())
SELECT (TIMESTAMP '2001-02-16 20:38:40' AT LOCAL) = toTimeZone(TIMESTAMP '2001-02-16 20:38:40', timeZone());

-- Timezone expression can be any expression, not just a literal
SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE concat('America', '/', 'Denver');

-- Precedence: AT TIME ZONE binds looser than arithmetic, so + is applied first
SELECT TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR AT TIME ZONE 'America/Denver';

-- Must equal the explicitly parenthesised form
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR AT TIME ZONE 'America/Denver')
     = toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');

-- Precedence: AT LOCAL also binds looser than arithmetic, so + is applied first
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR AT LOCAL)
     = toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, timeZone());

-- precedence / associativity (formatQuery pins the grouping)
SELECT formatQuery($$SELECT dt AT TIME ZONE 'UTC' = dt2$$);          -- toTimeZone(dt, 'UTC') = dt2 (binds tighter than '=')
SELECT formatQuery($$SELECT dt AT TIME ZONE 'A' AT TIME ZONE 'B'$$); -- toTimeZone(toTimeZone(dt, 'A'), 'B') (chained, left-assoc)
SELECT formatQuery($$SELECT t AT TIME ZONE tz FROM x$$);             -- toTimeZone(t, tz) (column operands)

-- error paths
SELECT 'x' AT TIME ZONE 'UTC';   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 AT FOO;                  -- { clientError SYNTAX_ERROR }

-- backward-compat of the new AT keyword: `at` / `local` as alias and column name
SELECT 1 AS at, number AS local FROM numbers(1);
