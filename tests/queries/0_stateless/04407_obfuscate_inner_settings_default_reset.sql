-- The analyzer applies `SETTINGS x = DEFAULT` to subquery scopes (not only `SETTINGS x = value`), so
-- resetting a setting to its default inside a nested `SELECT` actually takes effect for a table
-- function that reads that setting, consistent with how a non-default inner `SETTINGS` value is applied.
--
-- `obfuscate_markov_order` must be greater than zero or the `obfuscate` table function throws. Here an
-- invalid session value of 0 is reset back to its default (5) by an inner `SETTINGS
-- obfuscate_markov_order = DEFAULT`, so the obfuscation succeeds. Before the analyzer honored
-- `default_settings` for subquery scopes the table function still saw 0 and threw, while the query
-- result cache logic already treated such resets as effective, causing a mismatch.

SET allow_experimental_analyzer = 1;
SET obfuscate_markov_order = 0;

-- The inner DEFAULT resets the invalid session value back to 5, so the query succeeds.
SELECT count() FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8
    SETTINGS obfuscate_seed = 'stable', obfuscate_markov_order = DEFAULT);

-- Without the reset, the invalid session value still applies and the query fails.
SELECT count() FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8
    SETTINGS obfuscate_seed = 'stable'); -- { serverError BAD_ARGUMENTS }
