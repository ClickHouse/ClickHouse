-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: the test creates its own Replicated database and runs ON CLUSTER

-- A Replicated database and `ON CLUSTER` both hand the DDL to nodes that run it without the
-- submitting user, so the permission check has to happen on the node it was submitted to. See
-- 02346_text_index_transform_access for the checks themselves.

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

SELECT '3. ALTER ADD INDEX ON CLUSTER is denied.';

CREATE TABLE tab_on_cluster (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab_on_cluster ON CLUSTER test_shard_localhost
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '4. CREATE TABLE ON CLUSTER is denied on the legacy entry-format path.';

-- Below NORMALIZE_CREATE_ON_INITIATOR_VERSION the query is shipped as written and normalized by the
-- worker, which runs it with no user, so a separate check on the initiator covers that branch.
SET distributed_ddl_entry_format_version = 2;

CREATE TABLE tab_on_cluster_legacy ON CLUSTER test_shard_localhost
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SET distributed_ddl_entry_format_version = DEFAULT;

SELECT '5. Accepted with the privilege.';

SET allow_introspection_functions = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

ALTER TABLE tab_on_cluster ON CLUSTER test_shard_localhost
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

DROP TABLE tab_on_cluster;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} FORMAT Null;
