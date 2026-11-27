-- A trailing `SETTINGS` clause of a nested `INTERSECT` / `EXCEPT` is query-level, exactly as for a
-- `UNION`: the grammar leaves it on the last operand's `SELECT`, and it must shape the whole set
-- operation, including the operands that syntactically precede it.

SET enable_analyzer = 1;
SET obfuscate_markov_order = 0;

SELECT 'setting reset covers every operand of EXCEPT';
-- The invalid session value 0 is reset back to the default by the query-level `SETTINGS`.
-- The `obfuscate` call in the FIRST operand must see the reset too, otherwise it throws.
SELECT count() <= 4 FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4
    EXCEPT
    SELECT 'hello world'
    SETTINGS obfuscate_markov_order = DEFAULT);

SELECT 'setting change covers every operand of INTERSECT';
SELECT count() <= 4 FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4
    INTERSECT
    SELECT 'hello world'
    SETTINGS obfuscate_markov_order = 5);

SELECT 'without the query-level SETTINGS the invalid session value still applies';
SELECT count() FROM (
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(4)) LIMIT 4
    EXCEPT
    SELECT 'hello world'); -- { serverError BAD_ARGUMENTS }

SET obfuscate_markov_order = DEFAULT;

SELECT 'query parameter binding covers every operand';
SELECT count() FROM (
    SELECT {n:UInt8} AS x
    INTERSECT
    SELECT {n:UInt8} AS x
    SETTINGS param_n = '7');
