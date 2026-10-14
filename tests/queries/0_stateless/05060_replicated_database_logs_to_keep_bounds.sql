-- Tags: need-query-parameters

-- `logs_to_keep` of a Replicated database must not exceed `2147483647` (`Int32::max`), and a `CREATE`
-- naming a larger value is rejected instead of silently wrapping. The bound is below `UINT32_MAX`
-- because older replicas evaluate `entry_number + logs_to_keep` in 32-bit arithmetic, and any value
-- from 2^31 on can wrap there and make them delete the whole DDL log.

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

-- `UINT32_MAX` itself fits the 32-bit type, but on an older replica `entry_number + 4294967295` wraps
-- to `entry_number - 1`, so it is rejected too.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 4294967295; -- { serverError BAD_ARGUMENTS }

-- Just above the maximum.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 2147483648; -- { serverError BAD_ARGUMENTS }

-- The non-zero half of the type still holds.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 0; -- { serverError BAD_ARGUMENTS }

-- The maximum itself is in range.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05060', 's1', 'r1')
SETTINGS logs_to_keep = 2147483647;

SELECT value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/05060' AND name = 'logs_to_keep';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
