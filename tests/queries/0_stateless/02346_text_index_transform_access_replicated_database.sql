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

SELECT '5. So is an ALIAS column of the same query on the legacy entry-format path.';

CREATE TABLE tab_on_cluster_legacy ON CLUSTER test_shard_localhost
(
    id UInt64,
    val String,
    mangled String ALIAS demangle(val),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = mangled)
)
ENGINE = MergeTree ORDER BY id; -- { serverError FUNCTION_NOT_ALLOWED }

SET distributed_ddl_entry_format_version = DEFAULT;

SELECT '6. An alias renamed by an earlier command of the same ALTER is denied.';

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

SELECT '7. Also with ON CLUSTER.';

ALTER TABLE tab_on_cluster_alias ON CLUSTER test_shard_localhost
    RENAME COLUMN mangled TO renamed,
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = renamed) GRANULARITY 1; -- { serverError FUNCTION_NOT_ALLOWED }

DROP TABLE tab_on_cluster_alias;

SELECT '8. Indices inherited by CREATE TABLE AS ON CLUSTER are denied on the legacy entry-format path.';

-- The worker copies the source's indices with no user, so the initiator authorizes them.
SET allow_introspection_functions = 1;
CREATE TABLE tab_as_source
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val))
)
ENGINE = MergeTree ORDER BY id;
SET allow_introspection_functions = 0;

SET distributed_ddl_entry_format_version = 2;
CREATE TABLE tab_as_copy ON CLUSTER test_shard_localhost AS tab_as_source; -- { serverError FUNCTION_NOT_ALLOWED }
SET distributed_ddl_entry_format_version = DEFAULT;

DROP TABLE tab_as_source;

SELECT '9. ALTER ADD INDEX ON CLUSTER with a transform needs the table on the initiator.';

-- Without the table the initiator cannot expand an `ALIAS` column used in the transform.
ALTER TABLE tab_missing_on_initiator ON CLUSTER test_shard_localhost
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)) GRANULARITY 1; -- { serverError UNKNOWN_TABLE }

SELECT '10. Accepted with the privilege.';

SET allow_introspection_functions = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tab
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

ALTER TABLE tab_on_cluster ON CLUSTER test_shard_localhost
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = demangle(val)) GRANULARITY 1 FORMAT Null;

DROP TABLE tab_on_cluster;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} FORMAT Null;
