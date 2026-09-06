-- Tags: no-fasttest
-- Tag no-fasttest: needs the backups disk of the stateless configuration.

-- A Backup database locator persisted as a string literal is parsed a second time, by the engine
-- itself. That inner parse runs under the limits of the session that sent the locator, so a locator
-- written as a string cannot reach a parse depth the same locator written directly is refused.

SET max_parser_depth = 20;

DROP DATABASE IF EXISTS db_05062;

-- Refused as unparseable rather than opened, and the message names no part of the locator, which is
-- where a credential would sit.
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'', filename = [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]])'); -- { serverError BAD_ARGUMENTS }

-- A quoted locator within the limit still reaches the backup layer: that is what tells a locator
-- refused by the limit from one this server would open.
CREATE DATABASE db_05062 ENGINE = Backup('src', 'Disk(''backups'', ''05062_absent'')'); -- { serverError BACKUP_NOT_FOUND }

DROP DATABASE IF EXISTS db_05062;
