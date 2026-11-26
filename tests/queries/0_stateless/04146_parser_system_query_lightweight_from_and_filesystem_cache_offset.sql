-- Tags: no-parallel
-- ^^ required because the parser test mentions SYSTEM DROP subcommands. The
-- EXPLAIN SYNTAX wrapper means no command actually runs.
--
-- Exercise SYSTEM-query formatter branches not covered by 04117/04124:
--   * SYSTEM SYNC REPLICA ... LIGHTWEIGHT FROM 'r1', 'r2'
--     (the multi-source-replica comma-separated branch in ASTSystemQuery
--     formatImpl when sync_replica_mode == LIGHTWEIGHT && !src_replicas.empty()).
--   * SYSTEM DROP FILESYSTEM CACHE 'name' KEY xxx OFFSET N
--     (the OFFSET branch that prints offset_to_drop.value() after the KEY clause).
--   * SYSTEM DROP REPLICA 'r1' FROM SHARD 's1' (the !shard.empty() branch in
--     print_drop_replica that is taken when SHARD is provided).
--   * SYSTEM NOTIFY FAILPOINT fp (parallel to ENABLE/DISABLE, but uses a
--     separate Type::NOTIFY_FAILPOINT case in the formatter switch).

SELECT '--- SYSTEM SYNC REPLICA ... LIGHTWEIGHT FROM ... (multi-replica) ---';
EXPLAIN SYNTAX SYSTEM SYNC REPLICA db.t LIGHTWEIGHT FROM 'r1';
EXPLAIN SYNTAX SYSTEM SYNC REPLICA db.t LIGHTWEIGHT FROM 'r1', 'r2';
EXPLAIN SYNTAX SYSTEM SYNC REPLICA db.t LIGHTWEIGHT FROM 'r1', 'r2', 'r3';

SELECT '--- SYSTEM DROP FILESYSTEM CACHE with KEY + OFFSET ---';
EXPLAIN SYNTAX SYSTEM DROP FILESYSTEM CACHE 'my_cache' KEY xyz OFFSET 0;
EXPLAIN SYNTAX SYSTEM DROP FILESYSTEM CACHE 'my_cache' KEY xyz OFFSET 4096;

SELECT '--- SYSTEM DROP REPLICA ... FROM SHARD ---';
EXPLAIN SYNTAX SYSTEM DROP REPLICA 'r1' FROM SHARD 's1';
EXPLAIN SYNTAX SYSTEM DROP REPLICA 'r1' FROM SHARD 's1' FROM TABLE db.t;
EXPLAIN SYNTAX SYSTEM DROP REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '/clickhouse/zkpath';

SELECT '--- SYSTEM NOTIFY FAILPOINT ---';
EXPLAIN SYNTAX SYSTEM NOTIFY FAILPOINT fp;
