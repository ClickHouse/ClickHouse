-- A trailing `SETTINGS` clause of a UNION is query-level: it applies to the whole union, and the
-- grammar leaves it on the last arm (only the top-level query gets it as a separate node). It must
-- therefore shape every arm, including the non-last ones - the same contract
-- `InterpreterSetQuery::applySettingsFromQuery` implements for a directly executed query.

SET enable_analyzer = 1;
SET obfuscate_markov_order = 0;

SELECT 'setting reset covers every arm';
-- The invalid session value 0 is reset back to the default by the union's query-level `SETTINGS`.
-- The `obfuscate` call in the FIRST arm must see the reset too, otherwise it throws.
SELECT count() FROM (
    (SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4)
    UNION ALL
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4
    SETTINGS obfuscate_markov_order = DEFAULT);

SELECT 'setting change covers every arm';
SELECT count() FROM (
    (SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4)
    UNION ALL
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4
    SETTINGS obfuscate_markov_order = 5);

SELECT 'without the query-level SETTINGS the invalid session value still applies';
SELECT count() FROM (
    (SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4)
    UNION ALL
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4); -- { serverError BAD_ARGUMENTS }

SET obfuscate_markov_order = DEFAULT;

SELECT 'query parameter binding covers every arm';
SELECT sum(x) FROM (
    SELECT {n:UInt8} AS x
    UNION ALL
    SELECT {n:UInt8} AS x
    SETTINGS param_n = '7');

SELECT 'the last binding of a duplicated query parameter wins';
SELECT {n:UInt8} SETTINGS param_n = '1', param_n = '2';
