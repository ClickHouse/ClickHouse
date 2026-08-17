-- A `SETTINGS` clause of a `CREATE` may mix engine settings and query settings, so
-- `InterpreterSetQuery::applySettingsFromQuery` splits them and moves the query ones to the
-- context on a path of its own. A value-less `name` there stands for `name = true` and must be
-- rejected for a setting that is not Bool, on both halves of the split.

DROP TABLE IF EXISTS t_04663;

-- Query setting, moved to the context.
CREATE TABLE t_04663 (a UInt8) ENGINE = Memory SETTINGS max_threads; -- { error TYPE_MISMATCH }

-- Enum-valued query setting: a type mismatch, not a `BAD_GET` from casting `true` to its type.
CREATE TABLE t_04663 (a UInt8) ENGINE = Memory SETTINGS default_database_engine; -- { error TYPE_MISMATCH }

-- Engine setting, left in the storage definition.
CREATE TABLE t_04663 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity; -- { error TYPE_MISMATCH }

-- A Bool engine setting is what the shorthand is for. The value-less form is kept by the formatter,
-- so it is what `engine_full` shows.
CREATE TABLE t_04663 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS ttl_only_drop_parts;
SELECT extract(engine_full, 'SETTINGS [^,]*') FROM system.tables WHERE database = currentDatabase() AND name = 't_04663';

DROP TABLE t_04663;
