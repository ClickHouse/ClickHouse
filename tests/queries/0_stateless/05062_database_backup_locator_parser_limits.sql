-- Tags: no-fasttest
-- Tag no-fasttest: needs the backups disk of the stateless configuration.

-- A Backup database locator persisted as a string literal is parsed a second time, by the engine
-- itself. The statement that carries it is one string literal wherever the server measures a query,
-- so quoting a locator must not buy it a parse depth, a tree depth or a tree size that the same
-- locator written directly is refused.

DROP DATABASE IF EXISTS db_05062;

SET max_parser_depth = 20;

-- Refused as unparseable rather than opened, and the message names no part of the locator, which is
-- where a credential would sit.
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'', filename = [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]])'); -- { serverError BAD_ARGUMENTS }

-- A quoted locator within the limits still reaches the backup layer: that is what tells a locator
-- refused by a limit from one this server would open.
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'')'); -- { serverError BACKUP_NOT_FOUND }

SET max_parser_depth = 1000;
SET max_ast_depth = 20;

CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'', filename = toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(''x'')))))))))))))))))))))))))))))))))))))))))'); -- { serverError TOO_DEEP_AST }
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'')'); -- { serverError BACKUP_NOT_FOUND }

SET max_ast_depth = 1000;
SET max_ast_elements = 40;

CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'', filename = toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(toString(''x'')))))))))))))))))))))))))))))))))))))))))'); -- { serverError TOO_BIG_AST }
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'')'); -- { serverError BACKUP_NOT_FOUND }

DROP DATABASE IF EXISTS db_05062;
