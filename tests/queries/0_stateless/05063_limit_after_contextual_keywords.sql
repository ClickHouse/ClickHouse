-- `AFTER` and `UNTIL` are contextual keywords of `LIMIT`: an identifier with one of these names is
-- still a valid row count wherever a count is expected, because the keyword reading applies only when
-- an expression follows the word.
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after;
WITH 2 AS until SELECT number FROM numbers(5) ORDER BY number LIMIT until;
WITH 2 AS "after" SELECT number FROM numbers(5) ORDER BY number LIMIT "after";
WITH 2 AS after SELECT groupArray(number) FROM (SELECT number FROM numbers(6) ORDER BY number LIMIT after, 2);
WITH 2 AS after SELECT groupArray(number) FROM (SELECT number FROM numbers(6) ORDER BY number LIMIT 2 OFFSET after);
WITH 2 AS after SELECT groupArray(number) FROM (SELECT number FROM numbers(6) ORDER BY number LIMIT after BY number % 2);
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT after;
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number FETCH FIRST after ROWS ONLY;
WITH 2 AS after FROM numbers(6) |> ORDER BY number |> LIMIT after;
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after AS count_alias;
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after FETCH FIRST 3 ROWS ONLY;

-- The count reading formats back to the same query.
SELECT formatQuery('WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after');
SELECT formatQuery(formatQuery('WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after, 2'));
SELECT formatQuery(formatQuery('WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number FETCH FIRST after ROWS ONLY'));

-- When both readings are possible the keyword wins: `after(2)` is the range `AFTER (2)`, whose
-- constant-true boundary starts the output at the first row. Parentheses select the count reading.
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT after(2);
WITH 2 AS after SELECT number FROM numbers(5) ORDER BY number LIMIT (after);

-- The range forms themselves are unaffected.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 3;
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 3;
SELECT number FROM numbers(5) ORDER BY number LIMIT 2 AFTER number >= 1 UNTIL number >= 4;
SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT AFTER number >= 3;
SELECT number FROM numbers(5) ORDER BY number LIMIT 1 BY number LIMIT 1 AFTER number >= 3;

-- A boundary keyword without an expression falls back to the count reading of the bare word.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER; -- { serverError UNKNOWN_IDENTIFIER }
