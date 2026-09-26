-- The syntax of `SHOW TABLE SETTINGS` at its edges: names and patterns that need quoting, a name with too many
-- parts, the formatted form of each variant, and the statements that begin with the same words.

SELECT '-- formatted form: IN becomes FROM, and names that need backquotes keep them';
SELECT formatQuery('SHOW TABLE SETTINGS IN t');
SELECT formatQuery('SHOW CHANGED TABLE SETTINGS FROM `a.b`.`1c` NOT ILIKE \'x%\'');
SELECT formatQuery('SHOW TABLE SETTINGS FROM t LIKE \'it\'\'s\\\\\'');

SELECT '-- a name has at most two parts';
SELECT formatQuery('SHOW TABLE SETTINGS FROM a.b.c'); -- { serverError SYNTAX_ERROR }

SELECT '-- a table name and a pattern with a quote and a backslash reach the rewritten query intact';
DROP TABLE IF EXISTS `it's\\05241`;
CREATE TABLE `it's\\05241` (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 10;
SHOW TABLE SETTINGS FROM `it's\\05241` LIKE 'max_rows_to_keep';
SHOW TABLE SETTINGS FROM `it's\\05241` LIKE 'max_rows\'';
SHOW TABLE SETTINGS FROM `it's\\05241` LIKE 'x\' OR 1 = 1 --';

SELECT '-- FORMAT applies to the result';
SHOW CHANGED TABLE SETTINGS FROM `it's\\05241` FORMAT JSONEachRow;
DROP TABLE `it's\\05241`;

SELECT '-- SHOW CHANGED SETTINGS is still the statement about the session';
SET max_block_size = 12345;
SHOW CHANGED SETTINGS LIKE 'max_block_size';

SELECT '-- and SHOW TABLE <name> is still SHOW CREATE TABLE, also for a table called settings';
EXPLAIN AST SHOW TABLE settings;
