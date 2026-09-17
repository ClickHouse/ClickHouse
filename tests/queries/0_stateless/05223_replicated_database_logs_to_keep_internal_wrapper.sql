-- Tags: need-query-parameters

-- A definition the user supplies now is validated the same way whether the statement is executed
-- directly or through a wrapper that runs it as an internal query, such as `PARALLEL WITH`. Keying
-- the compatibility clamp on `internal` alone let such a `CREATE` through: the metadata file then held
-- the value as written while the database used `UINT32_MAX` in memory and in Keeper.
--
-- The partner statement has neither input nor output, which is what `PARALLEL WITH` requires.
--
-- Every case starts from a clean slate, so that a rejection that fails to happen does not turn the
-- next case into `DATABASE_ALREADY_EXISTS`.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05223', 's1', 'r1')
SETTINGS logs_to_keep = 10000000000
PARALLEL WITH
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.no_such_table; -- { serverError BAD_ARGUMENTS }

-- The rejection happens before anything is registered.
SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- A full-syntax `ATTACH` carries a user-written definition too, and with no metadata file on disk it
-- would become the definition of record, so it is rejected the same way.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} UUID '2b1f0b3e-5e0e-4c6a-9c1d-05223aaaaaaa'
ENGINE = Replicated('/test/' || currentDatabase() || '/05223', 's1', 'r1')
SETTINGS logs_to_keep = 10000000000
PARALLEL WITH
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.no_such_table; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
