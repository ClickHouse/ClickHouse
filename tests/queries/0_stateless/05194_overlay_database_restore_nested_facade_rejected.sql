-- Tags: no-parallel
-- This test creates databases with fixed global names and a `Memory` backup, so it cannot run
-- concurrently with copies of itself, like its `03611_overlay_database_*` siblings.

-- `RESTORE DATABASE` persists a database definition just like `CREATE DATABASE` does, so it must not
-- be able to write down a read-only `Overlay` facade nesting that no `CREATE` or `ATTACH` would
-- accept. Restore runs with `SECONDARY_CREATE` strictness, which only exempts the creation-time check
-- that every source database already exists (a facade and its sources are restored in the same
-- operation, in an unspecified order) - the rejection of one facade over another applies to restore
-- too, in both directions. Without it a restore would leave a facade with a source that
-- `resolveDatabases` has to drop on every lookup, so the facade would silently lose all tables of
-- that source.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/86768

DROP DATABASE IF EXISTS ov_restore_top;
DROP DATABASE IF EXISTS ov_restore_face;
DROP DATABASE IF EXISTS ov_restore_mid;
DROP DATABASE IF EXISTS ov_restore_src;
DROP DATABASE IF EXISTS ov_restore_base;

CREATE DATABASE ov_restore_src ENGINE = Atomic;
CREATE TABLE ov_restore_src.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_restore_src.t VALUES (1), (2);

CREATE DATABASE ov_restore_base ENGINE = Atomic;
CREATE TABLE ov_restore_base.b (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE DATABASE ov_restore_mid ENGINE = Atomic;
CREATE TABLE ov_restore_mid.m (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_restore_mid.m VALUES (3);

-- A facade over an ordinary database, and a separate facade to take a backup of.
CREATE DATABASE ov_restore_top ENGINE = Overlay('ov_restore_mid');
CREATE DATABASE ov_restore_face ENGINE = Overlay('ov_restore_src');

-- The facade reads its source.
SELECT count() FROM ov_restore_top.m;

BACKUP DATABASE ov_restore_face TO Memory('05194_overlay_restore_nested') FORMAT Null;

-- A facade cannot be restored under a name that another facade already uses as a source.
DROP DATABASE ov_restore_mid;
RESTORE DATABASE ov_restore_face AS ov_restore_mid FROM Memory('05194_overlay_restore_nested') FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- The name is still free, and re-creating it as an ordinary database works.
CREATE DATABASE ov_restore_mid ENGINE = Atomic;
CREATE TABLE ov_restore_mid.m (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_restore_mid.m VALUES (3), (4);
SELECT count() FROM ov_restore_top.m;

-- A facade cannot be restored over a source that is a facade itself.
DROP DATABASE ov_restore_face;
DROP DATABASE ov_restore_src;
CREATE DATABASE ov_restore_src ENGINE = Overlay('ov_restore_base');
RESTORE DATABASE ov_restore_face FROM Memory('05194_overlay_restore_nested') FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- `ATTACH DATABASE` is refused for the same shape, because it is user DDL that persists metadata.
ATTACH DATABASE ov_restore_face ENGINE = Overlay('ov_restore_src'); -- { serverError BAD_ARGUMENTS }

-- Restoring it over an ordinary source is still allowed.
DROP DATABASE ov_restore_src;
CREATE DATABASE ov_restore_src ENGINE = Atomic;
CREATE TABLE ov_restore_src.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ov_restore_src.t VALUES (1), (2), (3);
RESTORE DATABASE ov_restore_face FROM Memory('05194_overlay_restore_nested') FORMAT Null;
SELECT count() FROM ov_restore_face.t;

DROP DATABASE ov_restore_top;
DROP DATABASE ov_restore_face;
DROP DATABASE ov_restore_mid;
DROP DATABASE ov_restore_src;
DROP DATABASE ov_restore_base;
