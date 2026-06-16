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
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x REMOVE TTL'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x UInt16 FIRST'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t LIMIT 1 BY ALL'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a LIMIT 1 WITH TIES'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v SQL SECURITY DEFINER AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR OFFSET 30 MINUTE ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('WITH RECURSIVE x AS (SELECT 1) SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a CROSS JOIN b'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a NATURAL LEFT JOIN b'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a LEFT SEMI JOIN b ON a.x = b.y'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD STATISTICS a TYPE tdigest'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP STATISTICS a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t CLEAR STATISTICS a IN PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t CLEAR COLUMN x IN PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)'));

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

-- ---------------------------------------------------------------------------
-- SQL SECURITY: `DEFINER = CURRENT_USER` and an explicit `DEFINER = user` are mutually exclusive in
-- `ParserSQLSecurity`. With both set, `formatImpl` prints the explicit `definer` while
-- `processSQLSecurityOption` substitutes the current user, so the displayed definer would disagree
-- with the one access checks apply.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v DEFINER = u SQL SECURITY DEFINER AS SELECT 1'), '"definer":{', '"is_definer_current_user":true,"definer":{')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ALTER MODIFY COLUMN has exactly one parser sub-form (`REMOVE` / `MODIFY SETTING` / `RESET SETTING` /
-- `ADD ENUM VALUES` / plain modify). `formatImpl` prints only the first matching sub-form, but
-- `AlterCommand::parse` still applies the hidden fields, so a payload could execute a reorder or
-- setting reset the formatted SQL hides. `first`/`column` (AFTER) are valid only for the plain form.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x REMOVE TTL'), '"remove_property":"TTL"', '"remove_property":"TTL","first":true')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ALTER ADD INDEX: the parser produces either `FIRST` or `AFTER <index>`, never both. `formatImpl`
-- prints only `FIRST`, but `AlterCommand::apply` lets `after_index_name` override the `first`
-- position, so a payload with both would format as `ADD INDEX ... FIRST` while inserting after another.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t ADD INDEX idx a TYPE minmax AFTER b'), '"index":{', '"first":true,"index":{')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `LIMIT BY ALL` is parser-produced with an empty `limit_by` list (so it must round-trip), but a
-- *non-empty* explicit `LIMIT BY` list alongside `LIMIT BY ALL` is parser-impossible.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t LIMIT 1 BY b'), '"limit_by":', '"limit_by_all":true,"limit_by":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `LIMIT ... WITH TIES` requires an `ORDER BY` clause: `ParserSelectQuery` rejects it otherwise, and
-- `InterpreterSelectQuery` hits a `LOGICAL_ERROR` (`LIMIT WITH TIES without ORDER BY`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t ORDER BY a LIMIT 1 WITH TIES'), '"order_by":', '"unused_order_by":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `sql_security` is valid only for view shapes (`supportSQLSecurity()`). `formatImpl` hides it on a
-- plain `CREATE TABLE`, but `InterpreterCreateQuery::createTable` still runs `processSQLSecurityOption`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x UInt8) ENGINE = Memory'), '"attach":false', '"sql_security":{"type":"SQLSecurity","security_type":1,"is_definer_current_user":true},"attach":false')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- REFRESH strategy invariants (`ParserRefreshStrategy`): `OFFSET` is parsed only in the `EVERY`
-- branch and must be strictly less than the period. (`REFRESH DEPENDS ON` — `AFTER` with dependencies
-- and no period — is exercised as a valid round-trip above.)
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR OFFSET 30 MINUTE ENGINE = Memory AS SELECT 1'), '"schedule_kind":"EVERY"', '"schedule_kind":"AFTER"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR OFFSET 30 MINUTE ENGINE = Memory AS SELECT 1'), '"offset":{"type":"TimeInterval","seconds":1800', '"offset":{"type":"TimeInterval","seconds":7200')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTPartition`: the parser produces `PARTITION ALL` (`all`, no value/id) or a single `value`/`id`,
-- never `all` together with a value/id (`formatImpl` would emit `ALL` and silently drop the value).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"Partition","all":true,"value":{"type":"Literal","value":{"field_type":"UInt64","value":5}}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `recursive_with` is set by the parser only after parsing `WITH`; analysis trusts the flag regardless
-- (`AddDefaultDatabaseVisitor` does `select.with()->children` when set), so a flag without a `with`
-- child is parser-impossible and would reach a null dereference.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1'), '"type":"SelectQuery",', '"type":"SelectQuery","recursive_with":true,')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTTableJoin`: `ParserTablesInSelectQuery` parses `ON`/`USING` only for non-CROSS/comma/PASTE joins,
-- never for `NATURAL` joins (columns are derived), takes no strictness for CROSS/PASTE, and allows
-- SEMI/ANTI only for LEFT/RIGHT. A `clickhouse_json` payload that violates these would, e.g., format as
-- `CROSS JOIN ... ON ...` while the analyzer silently drops the predicate.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a INNER JOIN b ON a.x = b.y'), '"kind":"INNER"', '"kind":"CROSS"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a INNER JOIN b ON a.x = b.y'), '"kind":"INNER"', '"kind":"INNER","is_natural":true')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a LEFT SEMI JOIN b ON a.x = b.y'), '"kind":"LEFT"', '"kind":"INNER"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTWithElement`: a CTE body is parser-produced as an `ASTSubquery`, and `QueryTreeBuilder` hard-downcasts
-- it (`with_element->subquery->as<ASTSubquery &>()`). A `subquery` that is some other `ASTWithAlias`
-- (e.g. an identifier) formats fine but reaches that downcast as an internal error, so reject it.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"WithElement","name":"x","subquery":{"type":"Identifier","name":"y"}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ALTER ... STATISTICS: `ParserAlterCommand` produces a `TYPE` list only for `ADD`/`MODIFY STATISTICS`,
-- and never for `DROP`/`CLEAR`/`MATERIALIZE STATISTICS` (which carry only column names). `formatImpl`
-- prints the `TYPE` list for the add/modify forms and omits it otherwise, so an `ADD` without `types`
-- (formats `ADD STATISTICS a` with no `TYPE`) or a `DROP` carrying `types` is parser-impossible.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t ADD STATISTICS a TYPE tdigest'), '"types":{"type":"ExpressionList"', '"renamed":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP STATISTICS a'), '"statistics_decl":{"type":"StatisticsDeclaration","columns":', '"statistics_decl":{"type":"StatisticsDeclaration","types":{"type":"ExpressionList","children":[{"type":"Function","name":"tdigest","no_empty_args":true}]},"columns":')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ALTER ... IN PARTITION: `partition` is parser-produced only for the `CLEAR`/`MATERIALIZE` forms of
-- `DROP COLUMN`/`DROP INDEX`/`DROP STATISTICS`/`DROP PROJECTION` (where the corresponding `clear_*` flag
-- is set). A plain `DROP COLUMN` carrying a `partition` would format `DROP COLUMN x IN PARTITION p`,
-- which `ParserAlterCommand` never produces.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t CLEAR COLUMN x IN PARTITION 1'), '"clear_column":true', '"clear_column":false')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTWindowDefinition`: a frame `*_offset` expression is parser-produced only for an `Offset` boundary
-- (`<n> PRECEDING`/`<n> FOLLOWING`); `CURRENT ROW` and `UNBOUNDED` boundaries have no offset. `formatImpl`
-- prints the offset only for `Offset` boundaries, so an offset on a `Current`/`Unbounded` boundary is
-- parser-impossible and silently dropped on display.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT count() OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)'), '"frame_end_type":"Current"', '"frame_end_type":"Current","frame_end_offset":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }
