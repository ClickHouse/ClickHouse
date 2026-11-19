-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: the test creates its own Replicated database

-- A Replicated database hands the DDL to replicas that apply it without the submitting user, so the
-- permission check happens on the node it was submitted to. See 02346_text_index_transform_access
-- for the checks themselves, and 02346_text_index_transform_on_cluster for `ON CLUSTER`.

SET allow_introspection_functions = 0;

-- {CLICKHOUSE_DATABASE_2} is shared by every test in the client, hence the DROP.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier}
ENGINE = Replicated('/clickhouse/02346_text_index_transform_access/{database}', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab (id UInt64, val String)
ENGINE = MergeTree ORDER BY id FORMAT Null;

SELECT '1. ALTER ADD INDEX is denied.';

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '2. CREATE TABLE is denied.';

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab_denied
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '3. An alias renamed by an earlier command of the same ALTER is denied.';

-- The initiator has to authorize against the schema the whole statement produces: the hosts run it
-- without the user, and only after the rename does `renamed` expand to `demangle(val)`.
SET allow_introspection_functions = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab_alias (id UInt64, val String, mangled String ALIAS demangle(val))
ENGINE = MergeTree ORDER BY id FORMAT Null;
CREATE TABLE tab_on_cluster_alias (id UInt64, val String, mangled String ALIAS demangle(val)) ENGINE = MergeTree ORDER BY id;
SET allow_introspection_functions = 0;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab_alias
    RENAME COLUMN mangled TO renamed,
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = renamed) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '4. Accepted with the privilege.';

SET allow_introspection_functions = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} FORMAT Null;
