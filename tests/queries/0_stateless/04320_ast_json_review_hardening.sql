-- Regression tests for the AST JSON review hardening:
--  * the inherited query output suffix (FORMAT / INTO OUTFILE / SETTINGS) and the
--    `TEMPORARY` flag now survive the `parseQueryToJSON` -> `formatQueryFromJSON` round-trip;
--  * the two-argument form preserves keyword casing from the original text but takes
--    identifier casing from the AST, even when an identifier name collides with a keyword;
--  * malformed `clickhouse_json` is rejected at the JSON boundary instead of being coerced
--    or silently rewritten into a different valid statement.

-- Output suffix is preserved (previously dropped for these query types):
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW COLUMNS FROM t FORMAT JSON'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL FORMAT Null'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME TABLE a TO b FORMAT Null'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN SELECT 1 FORMAT JSON'));

-- `TEMPORARY` survives for both CREATE and DROP:
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TEMPORARY TABLE t'));

-- Valid shapes that the new validation must NOT reject (round-trips unchanged):
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a JOIN b USING (x)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a JOIN b ON a.x = b.x'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(\'^c\') APPLY(sum) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(\'^c\') APPLY(x -> x + 1) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t SAMPLE 1/3'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO DISK \'d\''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MOVE PARTITION 1 TO TABLE db.t2'));

-- Two-argument form: keyword casing from `original`, identifier casing from the AST,
-- even when the column name collides with a keyword (`Date`):
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT Date FROM t'), 'select date from t');

-- Strict scalar typing: a string where a boolean is expected must be rejected, not coerced:
SELECT formatQueryFromJSON('{"type":"SelectQuery","distinct":"yes"}'); -- { serverError BAD_ARGUMENTS }

-- A `Literal` requires an explicit `value`:
SELECT formatQueryFromJSON('{"type":"Literal"}'); -- { serverError BAD_ARGUMENTS }

-- Malformed `Field` payloads are rejected instead of collapsing to NULL / trusting the dump:
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"Null","value":"not-null"}}'); -- { serverError BAD_ARGUMENTS }
-- A `field_type` whose value restores to a different type is rejected by the type-match check:
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"Bogus","value":"\'x\'"}}'); -- { serverError BAD_ARGUMENTS }
-- A `field_type` whose value is not a restorable dump at all is rejected too:
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"Bogus","value":"1"}}'); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }

-- A typed list with a wrong child type is rejected at the boundary:
SELECT formatQueryFromJSON('{"type":"UserNamesWithHost","children":[{"type":"Identifier","name":"u"}]}'); -- { serverError BAD_ARGUMENTS }

-- `RENAME` requires a non-empty `elements` list:
SELECT formatQueryFromJSON('{"type":"RenameQuery"}'); -- { serverError BAD_ARGUMENTS }
