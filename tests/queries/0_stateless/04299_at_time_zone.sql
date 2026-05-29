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
