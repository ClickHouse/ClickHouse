-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: the test creates its own Replicated database

-- A Replicated database hands the DDL to replicas that apply it without the submitting user, so the
-- permission check happens on the node it was submitted to. See 02346_text_index_transform_access
-- for the checks themselves, and 02346_text_index_transform_on_cluster for `ON CLUSTER`.

SET allow_introspection_functions = 0;

-- {CLICKHOUSE_DATABASE_1} may be left over from a previous run, hence the DROP.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/clickhouse/02346_text_index_transform_access/{database}', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab (id UInt64, val String)
ENGINE = MergeTree ORDER BY id FORMAT Null;

SELECT '1. ALTER ADD INDEX is denied.';

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '2. CREATE TABLE is denied.';

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab_denied
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '3. Accepted with the privilege.';

SET allow_introspection_functions = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
