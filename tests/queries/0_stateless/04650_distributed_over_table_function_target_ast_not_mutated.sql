-- Analyzing the table-function target of a `Distributed` table must not rewrite the stored definition.
-- `TableFunctionFactory::get` runs `parseArguments`, which literalizes arguments in place (and, for object
-- storages, even erases an inline `SETTINGS` clause), so the analysis has to run on a copy of the target AST.

DROP TABLE IF EXISTS ast_src;
DROP TABLE IF EXISTS dist_inferred;
DROP TABLE IF EXISTS dist_explicit;

CREATE TABLE ast_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ast_src VALUES (1), (2), (3);

-- The column list is omitted, so the target is analyzed at `CREATE` to infer the structure.
CREATE TABLE dist_inferred ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), concat('^ast', '_src$')));

-- The table regexp must still be stored as written; only the database name is bound to the current database.
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_inferred';
SELECT sum(x) FROM dist_inferred;
-- Reading routes back to this server, which probes the target on the local replica - also on a copy.
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_inferred';

-- With an explicit column list the target is analyzed only to check the creator's access to it.
CREATE TABLE dist_explicit (x UInt64) ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), concat('^ast', '_src$')));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_explicit';
SELECT sum(x) FROM dist_explicit SETTINGS prefer_localhost_replica = 1;
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_explicit';

DROP TABLE dist_explicit;
DROP TABLE dist_inferred;
DROP TABLE ast_src;
