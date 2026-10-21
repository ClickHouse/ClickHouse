-- A table created from a table function builds its nested storage lazily, from the global context
-- captured at DDL time, so a persisted `CREATE TABLE ... AS obfuscate(...)` could never interpret its
-- query argument (it would fail with `THERE_IS_NO_QUERY` at read time). The stored definition would
-- also not be equivalent to the transient one, because `executeQuery` materializes the inner query's
-- query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` / `page`)
-- only for a directly executed query, not for a `CREATE` whose source is a table function. `obfuscate`
-- therefore rejects that DDL form, the same way `eval` does.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS t_obfuscate_persisted;
DROP VIEW IF EXISTS v_obfuscate;

SELECT '--- `CREATE TABLE ... AS obfuscate(...)` is rejected ---';
CREATE TABLE t_obfuscate_persisted AS obfuscate(SELECT number FROM numbers(100)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_obfuscate_persisted AS obfuscate(SELECT number FROM numbers(100) SETTINGS limit = 2); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_obfuscate_persisted';

SELECT '--- an ordinary view over `obfuscate` keeps working ---';
-- The view is expanded into the reading query, so the inner query is interpreted with a live query
-- context. `obfuscate` is an effectively infinite, repeating source, so the read needs a `LIMIT`.
CREATE VIEW v_obfuscate AS SELECT number FROM obfuscate(SELECT number FROM numbers(4));
SELECT count() FROM (SELECT * FROM v_obfuscate LIMIT 10);

SELECT '--- a view definition still rejects query-construction settings ---';
CREATE VIEW v_obfuscate_settings AS SELECT number FROM obfuscate(SELECT number FROM numbers(100) SETTINGS limit = 2); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_obfuscate;
