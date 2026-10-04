-- Tags: need-query-parameters

-- `logs_to_keep` of a Replicated database is bounded by the DDL log counter, which is 32-bit:
-- `DDLTaskBase::getLogEntryNumber` returns `UInt32` and `/max_log_ptr` is written from it. A larger
-- value could never take effect, so a `CREATE` naming one is rejected instead of silently wrapping.

-- Every case starts from a clean slate. Without that, a rejection that fails to happen leaves the
-- database behind, and the next case reports `DATABASE_ALREADY_EXISTS` instead of the rejection it
-- was checking for - which also stops the run before the remaining cases say anything.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 10000000000; -- { serverError BAD_ARGUMENTS }

-- Just above `UINT32_MAX`. This is the value that used to wrap to something small (4) and made the
-- cleanup thread delete almost the whole log.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 4294967300; -- { serverError BAD_ARGUMENTS }

-- The non-zero half of the type still holds.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 0; -- { serverError BAD_ARGUMENTS }

-- `UINT32_MAX` itself is in range.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 4294967295;

SELECT value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/05060' AND name = 'logs_to_keep';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
