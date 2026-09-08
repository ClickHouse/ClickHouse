-- `randConstant` holds one value for a whole query, so two syntactically identical calls must fold to
-- the same value. The analyzer achieves that by sharing one built `FunctionBase` between them, keyed by
-- tree hash, and that key must ignore aliases: renaming an expression cannot change the value the
-- function captures. Only the values are compared, never printed, so the rows are deterministic.

SELECT '-- two calls agree';
SELECT randConstant() = randConstant();

SELECT '-- two aliased calls agree';
SELECT a = b FROM (SELECT randConstant() AS a, randConstant() AS b);

SELECT '-- one aliased and one bare call agree';
SELECT c FROM (SELECT randConstant() AS a, a = randConstant() AS c);

SELECT '-- an alias inside the argument does not separate two calls';
SELECT a = b FROM (SELECT randConstant(1 AS one) AS a, randConstant(1 AS uno) AS b);

SELECT '-- two aliased calls over the same argument agree';
SELECT a = b FROM (SELECT randConstant(1) AS a, randConstant(1) AS b);

-- The argument is the documented way to ask for more than one constant in a query, so it must keep
-- separating calls. Four arguments rather than two, so the row cannot flake on a value collision: it
-- fails only if the four collapse to a single value, which is what sharing by name alone would do.
SELECT '-- different arguments still give different values';
SELECT length(arrayDistinct([randConstant(1), randConstant(2), randConstant(3), randConstant(4)])) >= 2;

SELECT '-- the value is still constant across rows';
SELECT uniqExact(randConstant()) = 1 FROM numbers(1000);

-- `now` is query-stable for a different reason (it reads the query time rather than drawing a value),
-- and `rand` is never folded, so both already agreed with and without aliases. They are here to show
-- the rows above are about the shared `FunctionBase` and not about aliases in general.
SELECT '-- now() agrees, aliased or not';
SELECT now() = now(), c FROM (SELECT now() AS a, now() AS b, a = b AS c);

SELECT '-- rand() agrees within one query, aliased or not';
SELECT rand() = rand(), c FROM (SELECT rand() AS a, rand() AS b, a = b AS c);

-- The cache spans query scopes, but stateful functions must not share an instance across them. The two
-- scalar subqueries must each run their own `blockNumber` counter, so they read the same number; a shared
-- instance would keep counting and hand the second one a larger number. The aliases keep the two subquery
-- trees apart, so the scalar cache cannot answer the second one from the first. Only the two numbers are
-- compared, never printed: where the counter starts depends on how many blocks the server has already
-- passed through this function, so the numbers themselves are not stable.
SELECT '-- stateful functions do not share instances across scopes';
SELECT (SELECT blockNumber() AS a FROM numbers(1)) = (SELECT blockNumber() AS b FROM numbers(1));

-- A function that is not deterministic in the scope of a query reads its value from the scope it is
-- evaluated in, so two scopes must never share one instance. `getSetting` and `getSettingOrDefault` see
-- the `SETTINGS` of their own subquery; with a shared `FunctionBase` the second subquery would report the
-- first one's setting value.
SELECT '-- getSetting does not share an instance across scopes';
SELECT (SELECT getSetting('max_threads') AS a SETTINGS max_threads = 1) != (SELECT getSetting('max_threads') AS b SETTINGS max_threads = 2);

SELECT '-- getSettingOrDefault does not share an instance across scopes';
SELECT (SELECT getSettingOrDefault('max_threads', 0) AS a SETTINGS max_threads = 1) != (SELECT getSettingOrDefault('max_threads', 0) AS b SETTINGS max_threads = 2);
