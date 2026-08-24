-- The `_temporary_and_external_tables` database holds the temporary tables of all sessions and all users,
-- and it is not covered by access control, so it must not be reachable through `Merge`.

CREATE TEMPORARY TABLE t_merge_temporary (dummy UInt8) ENGINE = Memory;
INSERT INTO t_merge_temporary VALUES (42);

-- A database regexp skips it, but still reads the other databases it matches.
-- Temporary tables are stored under generated names starting with `_tmp_`.
SELECT * FROM merge(REGEXP('^(_temporary_and_external_tables|system)$'), '^(one|_tmp_)') ORDER BY dummy;

-- The same for the `Merge` table engine.
CREATE TABLE t_merge_temporary_engine (dummy UInt8)
    ENGINE = Merge(REGEXP('^(_temporary_and_external_tables|system)$'), '^(one|_tmp_)');

SELECT * FROM t_merge_temporary_engine ORDER BY dummy;

DROP TABLE t_merge_temporary_engine;

-- When it is the only database that matches, there is nothing to read.
SELECT * FROM merge(REGEXP('^_temporary_and_external_tables$'), '^_tmp_'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Naming it explicitly is denied, the same way as direct access to it is.
SELECT * FROM merge('_temporary_and_external_tables', '^_tmp_'); -- { serverError DATABASE_ACCESS_DENIED }
SELECT * FROM _temporary_and_external_tables.t_merge_temporary; -- { serverError DATABASE_ACCESS_DENIED }

-- The same at `CREATE` time for the `Merge` engine: with an explicit column list, no read happens during `CREATE`,
-- so the unusable table definition would be stored otherwise.
CREATE TABLE t_merge_temporary_explicit (dummy UInt8)
    ENGINE = Merge('_temporary_and_external_tables', '^_tmp_'); -- { serverError DATABASE_ACCESS_DENIED }
