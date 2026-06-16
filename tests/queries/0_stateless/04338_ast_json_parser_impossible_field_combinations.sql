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
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY TTL d GROUP BY x SET y = max(y)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ORDER BY x WITH FILL INTERPOLATE (x AS x + 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() FROM t GROUP BY GROUPING SETS ((a), (b))'));

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

-- ---------------------------------------------------------------------------
-- ProjectionDeclaration has two parser-produced shapes: a `(SELECT ...)` projection (`query` set)
-- or an `INDEX ... TYPE ...` projection (`index` and `type` set together). An `index` without a
-- `projection_type` would format as `p INDEX a`, even though the parser requires `TYPE`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","index":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- BACKUP FROM SNAPSHOT: `base_snapshot_name` set means `ParserBackupQuery` skipped `parseElements`,
-- so the query carries no `elements` and `formatQueryImpl` prints the snapshot form. A payload that
-- sets both `base_snapshot_name` and `elements` is parser-impossible.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')'), '"base_snapshot_name":', '"elements":[{}],"base_snapshot_name":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- TTL GROUP BY: `group_by_key`/`group_by_assignments` are produced only by `ParserTTLElement` in the
-- `GROUP BY` branch, after at least one grouping key. A `GROUP_BY` mode with an empty `group_by_key`
-- would format `GROUP BY ` with no expressions, and these fields on a `DELETE` TTL are silently dropped.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d GROUP BY x SET y = max(y)'), '"group_by_key":[{"type":"Identifier","name":"x"}]', '"group_by_key":[]')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d'), '"mode":"DELETE"', '"mode":"DELETE","group_by_key":[{"type":"Identifier","name":"x"}]')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- INTERPOLATE is parser-produced only as an `ASTExpressionList` of `ASTInterpolateElement`s under an
-- `ORDER BY ... WITH FILL` clause. A non-`ASTInterpolateElement` child, or `interpolate` without a
-- `WITH FILL` order-by element (which `formatImpl` would silently drop), is parser-impossible.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT x FROM t ORDER BY x WITH FILL INTERPOLATE (x AS x + 1)'), '"type":"InterpolateElement"', '"type":"Asterisk"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT x FROM t ORDER BY x WITH FILL INTERPOLATE (x AS x + 1)'), '"with_fill":true', '"with_fill":false')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- GROUP BY GROUPING SETS: `group_by_with_grouping_sets` is produced only by
-- `GROUP BY GROUPING SETS (...)`, where every grouping set is a nested `ASTExpressionList`. The
-- analyzer does `group_asts[i]->as<const ASTExpressionList>()->children`, so setting the flag over an
-- ordinary `GROUP BY` (e.g. an `Identifier` child) is parser-impossible and must be rejected.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT count() FROM t GROUP BY a'), '"group_by":', '"group_by_with_grouping_sets":true,"group_by":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `group_by_with_constant_keys` is analysis-derived, not SQL syntax (a parsed AST never carries it),
-- so it is not deserialized from JSON: injecting it is silently ignored, and the query round-trips
-- without it instead of letting `clickhouse_json` lie about analysis state.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT count() FROM t GROUP BY a'), '"group_by":', '"group_by_with_constant_keys":true,"group_by":'));
