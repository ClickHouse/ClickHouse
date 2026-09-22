DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '--- a name that is not a setting at all is rejected ---';

-- The ordinary spelling, which every database engine already refused.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

-- `name = DEFAULT` is parsed into a different payload of the SETTINGS clause than `name = value`.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS max_tables = 10, not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

-- `param_x = ...` is parsed into a third payload, which no database engine reads.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS max_tables = 10, param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

-- A clause without an `ENGINE` describes the `Atomic` engine filled in by the interpreter.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
SETTINGS not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

SELECT '--- a setting of a different database engine is rejected ---';

-- `logs_to_keep` is a setting of `Replicated` only, and is no query setting, so the lookup being
-- per-engine rather than a union of all engines is what this arm measures.
SELECT count() FROM system.settings WHERE name = 'logs_to_keep';
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS logs_to_keep = DEFAULT; -- { serverError UNKNOWN_SETTING }

SELECT '--- a setting of the engine is still accepted in the reset form ---';

-- `engine_full` reads the stored clause back, so it is also the oracle for the reset being persisted:
-- where an accepted reset ends up is decided by the SETTINGS split, not by this check.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS max_tables = DEFAULT;
SELECT engine_full FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '--- a query setting is still accepted in the reset form ---';

-- `max_threads` is hoisted onto the query context, which empties the clause, so nothing is stored.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic
SETTINGS max_threads = DEFAULT;
SELECT engine_full FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '--- the second engine that accepts these settings behaves the same ---';

SET allow_deprecated_database_ordinary = 1;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary
SETTINGS not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary
SETTINGS max_tables = DEFAULT;
SELECT engine_full FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
