-- Tags: no-fasttest
-- A table name written as a query-parameter placeholder must survive fuzzing: `fuzzQuery` used to
-- fail with `UNKNOWN_TABLE` on such a query, and the placeholder must not be replaced by a concrete
-- database or table name either.
--
-- The oracle matches the parameter name only, because fuzzing rewrites a placeholder's declared type
-- but never its name, and it requires every row to keep it: once a row loses the placeholder every
-- later row does too, so a per-row check catches the rewrite wherever in the sequence it lands.
--
-- `ast_fuzzer_runs = 0`: the Stress test otherwise wraps every query with the server-side fuzzer,
-- which mutates the arguments below. `max_threads`/`max_block_size`: one stream of exactly 20 rows
-- keeps the seeded mutation sequence deterministic and the runtime bounded.

SELECT count() AS rows, countIf(position(query, '{p:') > 0) = count() AS placeholder_kept
FROM (SELECT query FROM fuzzQuery('SELECT count() FROM {p:Identifier}', 500, 100) LIMIT 20)
SETTINGS ast_fuzzer_runs = 0, max_threads = 1, max_block_size = 20;

SELECT count() AS rows, countIf(position(query, '{pdb:') > 0) = count() AS placeholder_kept
FROM (SELECT query FROM fuzzQuery('SELECT count() FROM {pdb:Identifier}.src', 500, 100) LIMIT 20)
SETTINGS ast_fuzzer_runs = 0, max_threads = 1, max_block_size = 20;
