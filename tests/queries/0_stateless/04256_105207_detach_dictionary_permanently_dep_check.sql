-- Regression test for issue #105207:
-- DETACH DICTIONARY ... PERMANENTLY SYNC used to call `flushAndShutdown` before
-- the dependency check, leaving the dictionary in a corrupted state when the
-- check failed: the table entry stayed in the catalog, but the dictionary
-- config had already been removed from `ExternalDictionariesLoader`, so any
-- subsequent `dictGet` against the still-attached dictionary failed with
-- `BAD_ARGUMENTS` "Dictionary not found".
--
-- The fix moves the dependency check before `flushAndShutdown`, matching what
-- `DROP TABLE` already does. After a failed DETACH PERMANENTLY, the dictionary
-- must remain fully usable.

DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d0;

CREATE DICTIONARY d0 (key UInt64, val Int) PRIMARY KEY key SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY d1 (key UInt64, val Int) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'd0')) LAYOUT(HASHED()) LIFETIME(0);

-- The failed DETACH PERMANENTLY must not leave `d0` in a half-detached state.
DETACH DICTIONARY d0 PERMANENTLY SYNC; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- The dictionary must still exist in the catalog.
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'd0';

-- The dictionary must still be reachable from `ExternalDictionariesLoader`,
-- so reading from it must not throw `BAD_ARGUMENTS` "Dictionary not found".
-- The `NULL` source has no rows so the lookup falls back to the default value `0`.
SELECT dictGet('d0', 'val', toUInt64(0));

-- The dependency check must also block plain (non-permanent) DROP DICTIONARY.
DROP DICTIONARY d0; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- And after that failed DROP, the dictionary must still be reachable.
SELECT dictGet('d0', 'val', toUInt64(0));

DROP DICTIONARY d1;
DROP DICTIONARY d0;
