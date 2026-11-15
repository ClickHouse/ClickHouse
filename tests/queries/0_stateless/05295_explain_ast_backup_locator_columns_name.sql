-- `EXPLAIN AST` hides the arguments of a `Backup` locator whatever the locator function is named:
-- a quoted name keeps its structure, a bare `COLUMNS` locator is hidden whole.

SET format_display_secrets_in_show_and_select = 0;

EXPLAIN AST CREATE DATABASE d ENGINE = Backup('db', `t.COLUMNS`('SEKRIT'));
EXPLAIN AST CREATE DATABASE d ENGINE = Backup('db', `COLUMNS`(1));
