-- A destination database that is full (its `max_tables` limit is reached) must not mask
-- source-side errors of `RENAME`: the source table is resolved and validated before the
-- destination quota is checked.

CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.occupant (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Atomic -> Atomic: a missing source is reported as unknown, not as a full destination.
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.missing TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError UNKNOWN_TABLE }
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.not_a_dictionary (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.not_a_dictionary TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError INCORRECT_QUERY }
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.not_a_dictionary;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.mv TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError NOT_IMPLEMENTED }

-- Ordinary -> Atomic: the same holds on the cross-engine rename path.
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.missing TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError UNKNOWN_TABLE }
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.not_a_dictionary (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.not_a_dictionary TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError INCORRECT_QUERY }

-- The quota still applies to renames of a valid source table.
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.not_a_dictionary TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError TOO_MANY_TABLES }
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.not_a_dictionary TO {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError TOO_MANY_TABLES }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
