-- Continuation of 04508_distributed_over_table_function (split because a single test must stay under the
-- flaky-check time limit): sharding-key semantics, ALTER guards, database binding and local-probe behavior
-- of a Distributed table over a table function.

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
-- `enable_parallel_replicas = 0` and `serialize_query_plan = 0` keep the read on the plain `Distributed`
-- path over the table's own (all-unavailable) cluster. Both are orthogonal execution variants that resolve
-- the remote target on the initiator, for the classic named-table form and the table-function form alike:
-- with parallel replicas the read is rewritten onto `cluster_for_parallel_replicas` (whose replicas are
-- reachable), and with `serialize_query_plan = 1` the initiator builds and serializes the full plan, which
-- resolves the target locally. In either case the missing target would raise `UNKNOWN_TABLE` on the
-- initiator instead of being skipped - so the skip behavior is asserted only on the plain path it applies to.
CREATE TABLE dist_probe_missing (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, loop(probe_missing_src));
CREATE TABLE dist_probe_missing_named (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, currentDatabase(), probe_missing_src);
SELECT count() FROM dist_probe_missing SETTINGS enable_analyzer = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SELECT count() FROM dist_probe_missing_named SETTINGS enable_analyzer = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SELECT count() FROM dist_probe_missing SETTINGS enable_analyzer = 0, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SELECT count() FROM dist_probe_missing_named SETTINGS enable_analyzer = 0, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
DROP TABLE dist_probe_missing;
DROP TABLE dist_probe_missing_named;

-- A dictionary missing only on the local replica must be treated as an absent backing object by the
-- local-replica probe too, so the query can fall back to a healthy remote replica or honor
-- `skip_unavailable_shards`. Unlike a missing table (`UNKNOWN_TABLE`), the `dictionary` table function
-- surfaces an unknown dictionary as `BAD_ARGUMENTS` ("... not found") from `ExternalDictionariesLoader`, so
-- the probe recognizes `BAD_ARGUMENTS` in addition to `UNKNOWN_TABLE` / `UNKNOWN_DATABASE`. Otherwise the
-- probe would abort the whole query on the local replica instead. `prefer_localhost_replica = 1` pins the
-- read to the local-replica probe path; `test_shard_localhost` has no remote replicas, so the (only, local)
-- shard is skipped and the query reports `ALL_CONNECTION_TRIES_FAILED` ("No available shards to query") -
-- byte for byte the same as a missing table on the same shard - rather than the fatal `BAD_ARGUMENTS`.
CREATE TABLE dist_probe_missing_dict (key UInt64, value String) ENGINE = Distributed(test_shard_localhost, dictionary('probe_missing_dict'));
SELECT count() FROM dist_probe_missing_dict SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM dist_probe_missing_dict SETTINGS enable_analyzer = 0, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }
-- Without `skip_unavailable_shards` (and with no remote replica to try) the missing dictionary still
-- surfaces the usual way, so nothing is silently swallowed.
SELECT count() FROM dist_probe_missing_dict SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, skip_unavailable_shards = 0, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE dist_probe_missing_dict;

-- A deterministic evaluation failure of the target table function is a real error and must be surfaced, not
-- silently downgraded to a skipped shard: the local-replica probe treats only a missing backing object
-- (`UNKNOWN_TABLE` / `UNKNOWN_DATABASE`, or `BAD_ARGUMENTS` for a missing dictionary) as "table absent",
-- every other exception propagates - even with `skip_unavailable_shards = 1`, which is only meant to swallow
-- unavailability / missing-table failures. `intDiv(1, 0)` raises `ILLEGAL_DIVISION` and fails the same way
-- whether the target runs on the local replica (the probe) or a remote shard, so the error surfaces
-- regardless of `prefer_localhost_replica`.
CREATE TABLE dist_probe_error (n UInt64) ENGINE = Distributed(test_shard_localhost, numbers(intDiv(1, 0)));
SELECT count() FROM dist_probe_error SETTINGS skip_unavailable_shards = 1; -- { serverError ILLEGAL_DIVISION }
DROP TABLE dist_probe_error;

-- The `BAD_ARGUMENTS` exemption above is narrow: it applies only to the `dictionary` table function (a
-- missing dictionary is reported as `BAD_ARGUMENTS`, there is no dedicated code). Because the table function
-- of a persisted `Distributed(..., table_function())` target is not validated with `parseArguments` at
-- `CREATE` when it has a static structure (see `registerStorageDistributed`), a target like `numbers(0, 10, 0)`
-- (zero step) can be created and only fail at read time with `BAD_ARGUMENTS`. That is a real configuration
-- error and must surface even on the local-replica probe path (`prefer_localhost_replica = 1`) and even with
-- `skip_unavailable_shards = 1`, not be downgraded to a skipped shard / `ALL_CONNECTION_TRIES_FAILED`.
CREATE TABLE dist_probe_bad_step (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers(0, 10, 0));
SELECT count() FROM dist_probe_bad_step SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM dist_probe_bad_step SETTINGS enable_analyzer = 0, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE dist_probe_bad_step;

-- `UNKNOWN_TABLE` / `UNKNOWN_DATABASE` mean "backing object missing" only when they come from resolving the
-- table function's OWN backing object, not from evaluating its arguments (argument evaluation is
-- node-independent, so a failure there is a deterministic definition error, not an absent local replica).
-- `numbers` has a static structure, so its arguments are evaluated only when the storage is materialized, not
-- during the structure-only probe: `numbers((SELECT count() FROM missing_src))` is created successfully (not
-- validated at `CREATE`, see `registerStorageDistributed`), and the `UNKNOWN_TABLE` its scalar subquery
-- raises for the missing `missing_src` must reach the user, not be downgraded to a skipped shard /
-- `ALL_CONNECTION_TRIES_FAILED` even on the local-replica probe path with `skip_unavailable_shards = 1`.
CREATE TABLE dist_probe_arg_subquery (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers((SELECT count() FROM missing_src)));
SELECT count() FROM dist_probe_arg_subquery SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM dist_probe_arg_subquery SETTINGS enable_analyzer = 0, prefer_localhost_replica = 1, skip_unavailable_shards = 1, enable_parallel_replicas = 0, serialize_query_plan = 0; -- { serverError UNKNOWN_TABLE }
DROP TABLE dist_probe_arg_subquery;

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
