-- Regression tests for the AST JSON review hardening of parser-impossible field combinations.
-- Some AST nodes carry fields that the SQL parser only ever sets in mutually exclusive
-- combinations. `readJSON` now rejects payloads that set them together (or inconsistently) at the
-- JSON boundary, so malformed `clickhouse_json` fails closed with `BAD_ARGUMENTS` instead of
-- building an AST whose displayed SQL disagrees with the operation actually executed.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t'));
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t PART \'all_1_1_0\''));
SELECT formatQueryFromJSON(parseQueryToJSON('KILL QUERY WHERE 1 SYNC'));
SELECT formatQueryFromJSON(parseQueryToJSON('KILL QUERY WHERE 1 ASYNC'));
SELECT formatQueryFromJSON(parseQueryToJSON('KILL MUTATION WHERE 1 TEST'));
SELECT formatQueryFromJSON(parseQueryToJSON('KILL PART_MOVE_TO_SHARD WHERE 1 TEST'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP REPLICA \'r\' FROM ZKPATH \'/clickhouse/tables/foo\''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP REPLICA \'r\' FROM ZKPATH \'aux:/clickhouse/foo\''));

-- ---------------------------------------------------------------------------
-- CHECK TABLE: `partition` and `part_name` are mutually exclusive (the parser produces either
-- `PARTITION <expr>` or `PART '<name>'`). A JSON AST carrying both would format both clauses while
-- `getPartitionOrPartitionID` executes against the partition only.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CHECK TABLE t PARTITION 1'), '"partition":', '"part_name":"all_1_1_0","partition":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- KILL: `SYNC`, `ASYNC` and `TEST` are mutually exclusive modes, so `sync` and `test` cannot both
-- be set. For `KILL PART_MOVE_TO_SHARD` such an AST would display as `TEST` while
-- `InterpreterKillQueryQuery` executes the unsupported sync variant.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('KILL PART_MOVE_TO_SHARD WHERE 1 SYNC'), '"sync":true', '"sync":true,"test":true')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- SYSTEM DROP REPLICA ... FROM ZKPATH: `zk_name` and `replica_zk_path` are derived from
-- `full_replica_zk_path` (one invariant). `formatImpl` prints `full_replica_zk_path` while the
-- interpreter operates on `replica_zk_path`/`zk_name`, so a payload where they disagree (or where
-- the derived fields appear without `full_replica_zk_path`) is rejected.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM DROP REPLICA \'r\' FROM ZKPATH \'aux:/clickhouse/foo\''), '"zk_name":"aux"', '"zk_name":"other"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM DROP REPLICA \'r\''), '"is_drop_whole_replica":true', '"zk_name":"x","is_drop_whole_replica":true')); -- { serverError BAD_ARGUMENTS }
