-- A Distributed table can be created over a table function, like the `cluster`/`remote` table functions.

DROP TABLE IF EXISTS dist_over_tf;
DROP TABLE IF EXISTS dist_over_tf_local;

-- The structure is inferred from the table function; single (local) shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT sum(number), count() FROM dist_over_tf;
-- The table function survives a metadata round-trip (it is re-parsed on ATTACH).
DETACH TABLE dist_over_tf;
ATTACH TABLE dist_over_tf;
SELECT sum(number), count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Two shards: the table function is executed on every shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Explicit columns and an optional sharding key are accepted.
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- A second argument that is not a registered table function is still treated as a database name,
-- so the classic `Distributed(cluster, database, table)` form (including `currentDatabase()`) is unaffected.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
INSERT INTO dist_over_tf_local VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
SELECT sum(x) FROM dist_over_tf;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- INSERT into a table-function-backed Distributed table is rejected (there is no concrete remote table).
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf VALUES (100); -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- Too many arguments for the table-function form.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10), number, 'default', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The `policy_name` parameter of the classic form is not accepted for the table-function form: the policy
-- only stores temporary files for background sends of `INSERT`s, which this read-only form rejects, and the
-- policy is resolved at `CREATE` / `ATTACH` / server startup, so accepting it would make a read-only table
-- fail to load on any node where the (unused) policy is absent.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10), rand(), 'default'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A `*Cluster` table function cannot back a table (`ITableFunctionCluster::canBeUsedToCreateTable` is false),
-- so it is rejected at create time, exactly as `CREATE TABLE ... AS urlCluster(...)` is - even when the
-- columns are given explicitly (otherwise the unsupported combination would only surface later at read time).
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE dist_over_tf (x String) ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
-- A user-issued `ATTACH TABLE ... (columns) ENGINE = ...` is a fresh query (`LoadingStrictnessLevel::ATTACH`),
-- not a load from previously-validated metadata, so it must be rejected too - otherwise it re-opens the bug
-- through a different entrypoint. Only server-startup / force-restore loads skip the check. (A `Memory`
-- database accepts `ATTACH TABLE` with an explicit definition without a UUID, unlike an `Atomic` one.)
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dist_over_tf (x String) ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A table function that resolves back to the Distributed table itself recurses, but the recursion is bounded
-- by `max_distributed_depth` (it does not hang): reading raises `TOO_LARGE_DISTRIBUTED_DEPTH`, the same way two
-- classic `Distributed` tables that reference each other do (self-references are only detected at create time
-- for the direct `Distributed(cluster, database, table)` form).
CREATE TABLE dist_over_tf (x UInt8) ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^dist_over_tf$'));
SELECT * FROM dist_over_tf SETTINGS max_distributed_depth = 3; -- { serverError TOO_LARGE_DISTRIBUTED_DEPTH }
DROP TABLE dist_over_tf;

-- A `dictGet` / `joinGet` in the sharding key of the table-function form is ignored by the engine (the key
-- does not apply - see `has_sharding_key` in `StorageDistributed`), so it must not register a loading
-- dependency on the dictionary. Otherwise, the ignored key would constrain the loading order at startup and
-- block `DROP` / `RENAME` of an object the engine never uses. `loading_dependencies_table` is empty.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers(10), dictGetUInt64('shard_dict', 'val', number));
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_tf';
DROP TABLE dist_over_tf;
DROP DICTIONARY shard_dict;

-- On the classic named-table form the sharding key is real, so a `dictGet` in it still registers a loading
-- dependency. This locks in that the skip above is specific to the table-function form.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local, dictGetUInt64('shard_dict', 'val', number));
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_tf';
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;
DROP DICTIONARY shard_dict;

-- The same holds for referential dependencies: a `dictGet` in the ignored sharding key of the
-- table-function form must not register a referential dependency, so the dictionary can be dropped even
-- with `check_referential_table_dependencies = 1`. Otherwise DROP / RENAME of an object the engine never
-- uses would be rejected, contradicting the documented read-only semantics of that form.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers(10), dictGetUInt64('shard_dict', 'val', number));
SET check_referential_table_dependencies = 1;
DROP DICTIONARY shard_dict;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;

-- On the classic named-table form the sharding key is real, so a `dictGet` in it still registers a
-- referential dependency and blocks dropping the dictionary. This locks in that the skip above is specific
-- to the table-function form.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local, dictGetUInt64('shard_dict', 'val', number));
SET check_referential_table_dependencies = 1;
DROP DICTIONARY shard_dict; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;
DROP DICTIONARY shard_dict;

-- The skip above must not hide a real dependency: a `dictGet` inside the target table function itself (not in
-- the ignored sharding key) is a referential dependency, but only when this node hosts a local replica of the
-- cluster and therefore runs the function locally. On a cluster with no local replicas the function runs only on
-- remote shards, so the object it reads is not a dependency of this node and can be dropped even with
-- `check_referential_table_dependencies = 1`. `test_cluster_multiple_nodes_all_unavailable` has no local replicas
-- (its replicas use a port that does not match this server), so the `dictGet` inside `numbers(...)` must not
-- become a dependency here.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, numbers(dictGetUInt64('shard_dict', 'val', 0)));
SET check_referential_table_dependencies = 1;
DROP DICTIONARY shard_dict;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;

-- On a cluster with a local replica the target table function runs locally, so the `dictGet` inside it is a real
-- referential dependency and blocks dropping the dictionary. This locks in the local-vs-remote distinction above
-- (and mirrors `cluster('c', ...)`, which also tracks a table-function target only for clusters with local replicas).
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers(dictGetUInt64('shard_dict', 'val', 0)));
SET check_referential_table_dependencies = 1;
DROP DICTIONARY shard_dict; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;
DROP DICTIONARY shard_dict;

-- A regexp target is different from a concrete-name target: the `merge` table function matches its regexp
-- against the catalog anew on every read, so the set of matched tables is dynamic by design and no referential
-- dependency is registered for them. Dropping a matched table does not break the definition - later reads
-- simply match the remaining tables (and tables created later join the set). This mirrors the `Merge` table
-- engine, which has never registered dependencies on the tables matched by its regexp: with both dependents
-- below still existing, the drop of the matched table succeeds even with
-- `check_referential_table_dependencies = 1`.
DROP TABLE IF EXISTS merge_src;
CREATE TABLE merge_src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE merge_classic (n UInt64) ENGINE = Merge(currentDatabase(), '^merge_src$');
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^merge_src$'));
SET check_referential_table_dependencies = 1;
DROP TABLE merge_src;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;
DROP TABLE merge_classic;

-- Restart safety: when the column list is omitted, the inferred structure is persisted in the table metadata
-- (like the classic named-table form), so at startup the table is re-created from that stored structure and
-- does not re-instantiate the target table function. `StorageDistributed`'s constructor calls
-- `getStructureOfRemoteTable` (which would resolve the target's backing objects locally) only when the column
-- list is empty, and it is never empty once loaded from metadata. So the table loads even if its backing
-- objects are not loaded yet, and no loading dependency on them is registered. `SHOW CREATE` shows the persisted
-- structure and `loading_dependencies_table` is empty. (The backing object is still a referential dependency,
-- as tested above, so it cannot be dropped while the table exists.)
DROP DICTIONARY IF EXISTS dep_dict;
DROP TABLE IF EXISTS dep_src;
CREATE TABLE dep_src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE DICTIONARY dep_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, loop(dep_src));
SHOW CREATE TABLE dist_over_tf;
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_tf';
DROP TABLE dist_over_tf;
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, dictionary('dep_dict'));
SHOW CREATE TABLE dist_over_tf;
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_tf';
DROP TABLE dist_over_tf;
DROP DICTIONARY dep_dict;
DROP TABLE dep_src;

-- `additional_table_filters` matched against the Distributed table cannot be propagated to the shards
-- when the target is a table function: the shard query reads from the table function, which has no named
-- source table to re-key the filter onto (its shard-side expression is referenced only by an internally
-- generated alias). It is rejected with a clear error instead of the confusing `UNKNOWN_TABLE`
-- ("Both table name and UUID are empty") that `main_table.getShortName()` produced on the empty source id.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT count() FROM dist_over_tf SETTINGS additional_table_filters = {'dist_over_tf': 'number > 5'}; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- The classic named-table form still supports `additional_table_filters` (the filter is re-keyed onto the
-- source table and applied on the shards): `number > 5` keeps 4 of the 10 rows, summing to 30.
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
INSERT INTO dist_over_tf_local SELECT * FROM numbers(10);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
SELECT count(), sum(number) FROM dist_over_tf SETTINGS additional_table_filters = {'dist_over_tf': 'number > 5'};
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- `INSERT ... SELECT` into a table-function-backed Distributed table is rejected with the same
-- `NOT_IMPLEMENTED` as the `INSERT ... VALUES` path, including with `parallel_distributed_insert_select`
-- enabled (its distributed fast paths would otherwise build an `INSERT` into an empty remote table id).
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_src ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf SELECT * FROM dist_over_tf_src SETTINGS parallel_distributed_insert_select = 2; -- { serverError NOT_IMPLEMENTED }
INSERT INTO dist_over_tf SELECT * FROM dist_over_tf_src SETTINGS parallel_distributed_insert_select = 1; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_src;
DROP TABLE dist_over_tf_local;

-- `INSERT ... SELECT` *from* a table-function-backed Distributed table with
-- `parallel_distributed_insert_select` keeps the projection and the filter of the original query:
-- the query shipped to the shards is rebuilt from the original `SELECT` with the table replaced by
-- the (aliased) table function, the same way it is done for a named-table source - not replaced with
-- a bare `SELECT * FROM table_function()`, which would silently drop them and write wrong rows.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst SELECT dist_over_tf.number + 1 FROM dist_over_tf WHERE number < 5 SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
-- The same holds for a `view` source (whose inner `SELECT` used to replace the whole query) read
-- through the `cluster` table function, which shares this code path.
TRUNCATE TABLE dist_over_tf_local;
INSERT INTO FUNCTION cluster('test_shard_localhost', currentDatabase(), dist_over_tf_local)
SELECT v.number * 2 FROM cluster('test_shard_localhost', view(SELECT number FROM numbers(10))) AS v WHERE v.number >= 8
SETTINGS parallel_distributed_insert_select = 2;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- The projection/filter preservation must also work for a database-qualified source reference
-- (`db.dist_over_tf.number`): the shard-side rewrite restores the qualified name onto the table-function
-- alias, the same way the legacy read path does, otherwise `db.dist_over_tf.number` would dangle against
-- `numbers(10) AS dist_over_tf` on the shard and fail to resolve.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number + 1
FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf
WHERE number < 5
SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- The qualified-asterisk form (`db.dist_over_tf.*`) must be rewritten onto the shard-side alias in the
-- parallel `INSERT ... SELECT` fast path too: it is an `ASTQualifiedAsterisk`, whose qualifier is a whole
-- table reference rather than an `ASTIdentifier` column, so the database qualifier would otherwise dangle
-- against `numbers(10) AS dist_over_tf` on the shard and the fast path would fail at execution time.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.*
FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf
SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- `distributed_product_mode = 'local'` rewrites a nested Distributed subquery to its concrete remote table.
-- A table-function-backed Distributed table has no such table, so the rewrite is rejected with a clear
-- `NOT_IMPLEMENTED` instead of failing deep inside the rewrite on an empty table id. Covered for both the
-- analyzer (`buildQueryTreeForShard`) and the old analyzer (`InJoinSubqueriesPreprocessor`); needs >= 2 shards.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- Qualified column references against a table-function-backed Distributed table must resolve on the shard
-- on the legacy (enable_analyzer = 0) read path too. That path replaces the `FROM` clause with the table
-- function, so the table function is aliased with the same qualifier the original query used for the
-- Distributed table - its name, or an explicit alias - and qualified references (including the
-- database-qualified `db.table.column` form) are restored onto that alias, otherwise they would
-- dangle on the shard. (The analyzer path resolves columns structurally; it is covered as well.)
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT sum(dist_over_tf.number) FROM dist_over_tf SETTINGS enable_analyzer = 0;
SELECT sum(d.number) FROM dist_over_tf AS d SETTINGS enable_analyzer = 0;
SELECT sum({CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 0;
SELECT sum(dist_over_tf.number) FROM dist_over_tf SETTINGS enable_analyzer = 1;
SELECT sum(d.number) FROM dist_over_tf AS d SETTINGS enable_analyzer = 1;
SELECT sum({CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 1;
DROP TABLE dist_over_tf;

-- The qualified-*asterisk* form (`db.dist_over_tf.*`) is an `ASTQualifiedAsterisk`, whose qualifier is a
-- whole table reference rather than an `ASTIdentifier` column, so `RestoreQualifiedNamesVisitor` (which only
-- rewrites identifiers) leaves it untouched and `db.dist_over_tf.*` dangles against `numbers(...) AS
-- dist_over_tf` on the shard. The asterisk qualifier must be rewritten onto the alias as well. Covered for the
-- table-qualified, alias-qualified and database-qualified forms on the legacy path (and the analyzer path,
-- which resolves it structurally, as a control). A three-row source keeps the reference small.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(3));
SELECT dist_over_tf.* FROM dist_over_tf ORDER BY number SETTINGS enable_analyzer = 0;
SELECT d.* FROM dist_over_tf AS d ORDER BY number SETTINGS enable_analyzer = 0;
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.* FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf ORDER BY number SETTINGS enable_analyzer = 0;
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.* FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf ORDER BY number SETTINGS enable_analyzer = 1;
DROP TABLE dist_over_tf;

-- A `sharding_key` passed to the table-function form is not a real shard map: every shard runs the same
-- table function and returns the same rows, so the key must not drive read optimizations. With
-- `optimize_skip_unused_shards`, `WHERE number = 1` must still query every shard (one matching row per shard,
-- i.e. 2, not 1), and with `optimize_distributed_group_by_sharding_key` the final cross-shard merge must
-- still run (a single `1 2` group, not a `1 1` finalized per shard). Covered for both analyzers.
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
SELECT count() FROM dist_over_tf WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 1;
SELECT count() FROM dist_over_tf WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 0;
SELECT number, count() FROM dist_over_tf WHERE number = 1 GROUP BY number ORDER BY number SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, enable_analyzer = 1;
SELECT number, count() FROM dist_over_tf WHERE number = 1 GROUP BY number ORDER BY number SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, enable_analyzer = 0;
DROP TABLE dist_over_tf;

-- The disable above is scoped to the persisted `Distributed(...)` engine over a table function. The
-- `remote`/`cluster`/`clusterAllReplicas` table functions also build a `Distributed` over a table function,
-- but they own their cluster and have always used a sharding key for shard skipping - a long-standing feature
-- (see `01930_optimize_skip_unused_shards_rewrite_in`, `01952_optimize_distributed_group_by_sharding_key`) -
-- so that must keep working. Here `optimize_skip_unused_shards` still prunes `WHERE number = 1` to a single
-- shard and returns 1, not 2. Covered for both analyzers.
SELECT count() FROM cluster('test_cluster_two_shards_localhost', numbers(10), number) WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 1;
SELECT count() FROM cluster('test_cluster_two_shards_localhost', numbers(10), number) WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 0;

-- Because the sharding key is ignored for the table-function form, `checkAlterIsPossible` does not enforce its
-- numeric type, so an incompatible `MODIFY COLUMN` of the column it mentions is allowed. But the key is stored
-- verbatim in the engine definition and `ALTER` does not rewrite it, so `RENAME` / `DROP` of a column referenced
-- by the key is rejected: otherwise `SHOW CREATE TABLE` and the persisted metadata would reference a column that
-- no longer exists. An `ALTER` of an unrelated column is unaffected.
CREATE TABLE dist_over_tf (number UInt64, x String) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
ALTER TABLE dist_over_tf MODIFY COLUMN number String;
ALTER TABLE dist_over_tf RENAME COLUMN number TO n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE dist_over_tf DROP COLUMN number; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE dist_over_tf RENAME COLUMN x TO y;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'dist_over_tf' ORDER BY name;
DROP TABLE dist_over_tf;

-- On the classic named-table form the sharding key is real, so the same `ALTER` on the key column is still
-- rejected: this locks in that the validation is only skipped for the table-function form.
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local, number);
ALTER TABLE dist_over_tf RENAME COLUMN number TO n; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- Only the real source columns of the ignored key are protected against a stale reference, not every syntactic
-- identifier: a lambda parameter that happens to share a name with a real column must not block dropping that
-- column. Here `x` appears in the key only as the lambda variable of `arrayExists`, so `DROP COLUMN x` is
-- allowed, while `arr` is genuinely referenced by the key, so renaming / dropping it is still rejected. The
-- extra column `y` keeps the table non-empty so dropping `arr` reaches the sharding-key guard rather than the
-- generic "cannot drop all columns" check.
CREATE TABLE dist_over_tf (x UInt64, arr Array(UInt64), y String) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), arrayExists(x -> x = 1, arr));
ALTER TABLE dist_over_tf DROP COLUMN x;
ALTER TABLE dist_over_tf RENAME COLUMN arr TO arr2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE dist_over_tf DROP COLUMN arr; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'dist_over_tf' ORDER BY name;
DROP TABLE dist_over_tf;

-- The target table function is bound to the current database at CREATE time: `currentDatabase()` inside it is
-- replaced with its value, unqualified table identifiers in a nested subquery are qualified, and the qualified
-- form is persisted - the same normalization `CREATE VIEW` applies to its stored SELECT, and the table-function
-- analogue of how the classic form evaluates its `database` argument to a literal. Otherwise the stored target
-- would be resolved against the current database of whatever session queries the table, so the same table would
-- silently read different data depending on the caller (or fail on a remote shard, where the session database
-- is the connection default). Verified by querying from a session whose current database is a different one
-- holding decoy tables of the same name, over both the local fast path and the serialized shard query
-- (`prefer_localhost_replica = 0`), with both analyzers. The DETACH/ATTACH round-trip from the other database
-- checks that re-running the normalization on the already-qualified persisted form is a no-op.
-- The binding also walks the target recursively, so a table function nested inside a subquery (or inside
-- another table function's argument) is bound too, not only the outermost target: `dist_over_tf_nested`
-- persists `merge('default', '^bind_src$')` inside the scalar subquery, so it keeps reading the creating
-- database from any session (analyzer only, as the legacy path cannot evaluate the scalar subquery).
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^bind_src$'));
CREATE TABLE dist_over_tf_subq ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM bind_src))));
CREATE TABLE dist_over_tf_nested ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM merge('^bind_src$')))));
SHOW CREATE TABLE dist_over_tf;
SHOW CREATE TABLE dist_over_tf_subq;
SHOW CREATE TABLE dist_over_tf_nested;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_src VALUES (100), (200), (300), (400), (500);
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 0, prefer_localhost_replica = 0;
-- (analyzer only: evaluating a scalar subquery in a table function argument at read time is a pre-existing
-- limitation of the legacy path, and without `assumeNotNull` of both paths - `cluster()` fails the same way)
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf_subq SETTINGS enable_analyzer = 1;
-- The nested `merge('^bind_src$')` is bound to the creating database, so it reads the 3 rows there, not the
-- 5 rows of the decoy `bind_src` in the current (querying) database.
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf_nested SETTINGS enable_analyzer = 1;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_over_tf_subq;
DROP TABLE dist_over_tf_nested;
DROP TABLE dist_over_tf;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE bind_src;

-- The `dictionary` table function resolves an unqualified dictionary name against the current database at
-- read time, exactly like `dictGet` does (`TableFunctionDictionary::getActualTableStructure`), and
-- `AddDefaultDatabaseVisitor` above already freezes `dictGet`'s dictionary name argument for that reason - so
-- the table function's own argument is qualified the same way when it is itself the target being bound,
-- otherwise `Distributed(cluster, dictionary('d'))` would still depend on the current database of whatever
-- session queries the table later. Verified by querying from a different current database holding a decoy
-- dictionary of the same name.
DROP DICTIONARY IF EXISTS dict_src;
CREATE DICTIONARY dict_src (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 1 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, dictionary('dict_src'));
SHOW CREATE TABLE dist_over_tf;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict_src (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 2 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT val FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_over_tf;
DROP DICTIONARY dict_src;
DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `joinGet`'s first argument names a `Join`-engine table, resolved against the current database at read time
-- (`FunctionJoinGet` -> `Context::resolveStorageID`) unless already qualified, exactly like `dictGet`'s
-- dictionary name argument - `AddDefaultDatabaseVisitor` now qualifies it the same way, so it is frozen at
-- CREATE time here too, otherwise `Distributed(cluster, numbers(joinGet('j', ...)))` would still depend on the
-- current database of whatever session queries the table later. Verified by querying from a different current
-- database holding a decoy `join_src` of the same name.
CREATE TABLE join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_src VALUES (0, 1);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(joinGet('join_src', 'v', toUInt64(0))));
SHOW CREATE TABLE dist_over_tf;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.join_src VALUES (0, 5);
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_over_tf;
DROP TABLE join_src;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.join_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The one-argument form of the `merge` table function matches its table name regexp against the tables of
-- the current database at read time, so the database is bound at CREATE time like the other table-function
-- targets above. Verified by querying from a different current database holding a decoy `bind_src` of the
-- same name.
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, merge('^bind_src$'));
SHOW CREATE TABLE dist_over_tf;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_src VALUES (100), (200), (300);
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_over_tf;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE bind_src;

-- The one-argument form of the `loop` table function resolves an unqualified table name against the current
-- database at read time (`TableFunctionLoop` falls back to the current database), so the database is bound
-- at CREATE time. `loop` reads its backing table in an endless cycle, so the read is capped with LIMIT.
-- Verified by querying from a different current database holding a decoy `loop_src` of the same name.
CREATE TABLE loop_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO loop_src VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, loop(loop_src));
SHOW CREATE TABLE dist_over_tf;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.loop_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.loop_src VALUES (100), (200), (300);
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT sum(n) FROM (SELECT n FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf LIMIT 3);
USE {CLICKHOUSE_DATABASE:Identifier};

-- The table named by the persisted `loop` target is a referential dependency of the `Distributed` table:
-- with `check_referential_table_dependencies = 1` it cannot be dropped or renamed away from under it.
SET check_referential_table_dependencies = 1;
DROP TABLE loop_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
RENAME TABLE loop_src TO loop_src2; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;

-- No dependency is registered when the cluster has no local replicas: the target table function runs only
-- on remote shards (mirrors the `dictGet` case above). The columns are explicit because inferring the
-- structure needs an available shard.
CREATE TABLE dist_over_tf (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, loop(loop_src));
SET check_referential_table_dependencies = 1;
DROP TABLE loop_src;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_tf;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.loop_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Reading a persisted `Distributed` over a table function must not resolve the target on the initiator:
-- the engine has its own declared column list and the target is meant to run only on the shards. With the
-- modern analyzer (`enable_analyzer = 1`) the read now builds its header from the declared columns and reaches
-- shard dispatch instead of failing on the initiator while resolving a target whose backing object is missing
-- there. Over an all-unavailable cluster the read is then skipped under `skip_unavailable_shards` and returns
-- an empty result, exactly like the classic named-table form, rather than throwing `UNKNOWN_TABLE` while
-- resolving the target on the initiator. The legacy analyzer builds the header from the declared columns too.
-- These queries produce no rows; the point is that they succeed instead of failing on the initiator.
-- `enable_parallel_replicas = 0` keeps the read on the table's own (all-unavailable) cluster: with parallel
-- replicas the read is rewritten onto `cluster_for_parallel_replicas`, whose replicas are reachable, so the
-- missing target would resolve there and raise `UNKNOWN_TABLE` instead of being skipped.
CREATE TABLE dist_probe_missing (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, loop(probe_missing_src));
CREATE TABLE dist_probe_missing_named (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, currentDatabase(), probe_missing_src);
SELECT count() FROM dist_probe_missing SETTINGS enable_analyzer = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0;
SELECT count() FROM dist_probe_missing_named SETTINGS enable_analyzer = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0;
SELECT count() FROM dist_probe_missing SETTINGS enable_analyzer = 0, skip_unavailable_shards = 1, enable_parallel_replicas = 0;
SELECT count() FROM dist_probe_missing_named SETTINGS enable_analyzer = 0, skip_unavailable_shards = 1, enable_parallel_replicas = 0;
DROP TABLE dist_probe_missing;
DROP TABLE dist_probe_missing_named;

-- A deterministic evaluation failure of the target table function is a real error and must be surfaced, not
-- silently downgraded to a skipped shard: the local-replica probe only treats a missing backing object
-- (`UNKNOWN_TABLE` / `UNKNOWN_DATABASE`) as "table absent", every other exception propagates - even with
-- `skip_unavailable_shards = 1`, which is only meant to swallow unavailability / missing-table failures.
-- `intDiv(1, 0)` fails the same way whether the target runs on the local replica (the probe) or a remote
-- shard, so the error surfaces regardless of `prefer_localhost_replica`.
CREATE TABLE dist_probe_error (n UInt64) ENGINE = Distributed(test_shard_localhost, numbers(intDiv(1, 0)));
SELECT count() FROM dist_probe_error SETTINGS skip_unavailable_shards = 1; -- { serverError ILLEGAL_DIVISION }
DROP TABLE dist_probe_error;

-- The MergeTree-inspection table functions (`mergeTreeIndex`, `mergeTreeProjection`, `mergeTreeTextIndex`,
-- `mergeTreeAnalyzeIndexes`) name a concrete local table in their first two arguments and read it at query
-- time, so a persisted `Distributed` over such a target registers a referential dependency on that table -
-- DROP / RENAME of the source is rejected under `check_referential_table_dependencies = 1`, and allowed once
-- the dependent `Distributed` tables are gone. The UUID-resolved `mergeTreeAnalyzeIndexesUUID` form references
-- its source by UUID rather than by name and so is intentionally not covered here.
CREATE TABLE mt_src (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dist_over_mt_proj (a UInt64) ENGINE = Distributed(test_shard_localhost, mergeTreeProjection(currentDatabase(), mt_src, p));
CREATE TABLE dist_over_mt_index (a UInt64) ENGINE = Distributed(test_shard_localhost, mergeTreeIndex(currentDatabase(), mt_src));
CREATE TABLE dist_over_mt_text (a UInt64) ENGINE = Distributed(test_shard_localhost, mergeTreeTextIndex(currentDatabase(), mt_src, idx));
CREATE TABLE dist_over_mt_analyze (a UInt64) ENGINE = Distributed(test_shard_localhost, mergeTreeAnalyzeIndexes(currentDatabase(), mt_src));
SET check_referential_table_dependencies = 1;
DROP TABLE mt_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
RENAME TABLE mt_src TO mt_src2; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;
DROP TABLE dist_over_mt_proj;
DROP TABLE dist_over_mt_index;
DROP TABLE dist_over_mt_text;
DROP TABLE dist_over_mt_analyze;
DROP TABLE mt_src;
