-- Exercise all identifier-quoting combinations accepted by
-- `SYSTEM CLEAR ... FOR TABLE`. `EXPLAIN SYNTAX` parses and formats the AST
-- without executing the cache-clearing commands.
SELECT '--- FOR TABLE identifier quoting ---';
EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FOR TABLE db.table;
EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FOR TABLE `db`.`table`;
EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FOR TABLE `db`.table;
EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FOR TABLE db.`table`;
EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FOR TABLE `база`.`ётаблица`;
