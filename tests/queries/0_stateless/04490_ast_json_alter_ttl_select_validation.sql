-- Regression test for the AST JSON review hardening of ALTER / TTL / SELECT shapes.
-- Covers:
--   * `ALTER TABLE ... ATTACH PART '...' FROM '...'` now round-trips its source path (it was dropped
--     by `formatImpl`, hiding the path that `clickhouse_json` execution would still consume).
--   * `MATERIALIZE STATISTICS ALL` / `CLEAR STATISTICS ALL` are parser-produced with a null
--     declaration; `readJSON` must accept its own writer's JSON for those `ALL` forms.
--   * Per-command scalar fields required by the parser (`FETCH ... FROM`, `[ATTACH|REPLACE] PARTITION
--     ... FROM`, `UNFREEZE ... WITH NAME`) are validated at the JSON boundary.
--   * `TTL MOVE` only accepts a `DISK`/`VOLUME` destination with a non-empty name; other destination
--     state from malformed JSON is rejected instead of hitting a `LOGICAL_ERROR` / empty target.
--   * non-array TTL `group_by_key` / `group_by_assignments` are rejected (not silently dropped).
--   * `cte_aliases` is restored as an `ASTExpressionList` of identifiers, like the column `aliases`.

-- ---------------------------------------------------------------------------
-- Valid shapes the strict validation / round-trip must NOT reject.
-- ---------------------------------------------------------------------------

-- `ATTACH PART ... FROM ...`: the source path must survive the round trip now.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ATTACH PART \'p\' FROM \'/path\''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ATTACH PART \'p\''));

-- `[ATTACH|REPLACE] PARTITION ... FROM` source table.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ATTACH PARTITION 1 FROM src'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t REPLACE PARTITION 1 FROM src'));

-- `FETCH PARTITION ... FROM '<path>'`.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t FETCH PARTITION 1 FROM \'/zk\''));

-- `UNFREEZE ... WITH NAME '<name>'`.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t UNFREEZE PARTITION 1 WITH NAME \'b\''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t UNFREEZE WITH NAME \'b\''));

-- `MATERIALIZE STATISTICS ALL` / `CLEAR STATISTICS ALL` (null declaration) and a plain decl form.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE STATISTICS ALL'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t CLEAR STATISTICS ALL'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP STATISTICS a'));

-- `TTL ... TO DISK '<name>'` (mode MOVE).
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY TTL d TO DISK \'fast\''));

-- `WITH <cte>(<aliases>) AS (...)` column aliases.
SELECT formatQueryFromJSON(parseQueryToJSON('WITH cte1(zzz) AS (SELECT 1) SELECT 1 FROM cte1'));

-- ---------------------------------------------------------------------------
-- Malformed JSON shapes that must fail closed with BAD_ARGUMENTS at the boundary.
-- ---------------------------------------------------------------------------

-- `TTL MOVE` to a non-DISK/VOLUME destination would reach a LOGICAL_ERROR in formatting.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d TO DISK \'fast\''), '"destination_type":"DISK"', '"destination_type":"DELETE"')); -- { serverError BAD_ARGUMENTS }
-- `TTL MOVE` with an empty destination name would format an empty target.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d TO DISK \'fast\''), ',"destination_name":"fast"', '')); -- { serverError BAD_ARGUMENTS }

-- non-array TTL `group_by_key` must be rejected, not silently dropped.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d GROUP BY x SET y = max(y)'), '"group_by_key":[', '"group_by_key":{},"_unused":[')); -- { serverError BAD_ARGUMENTS }

-- `FETCH PARTITION` without the `FROM` path.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t FETCH PARTITION 1 FROM \'/zk\''), ',"from":"/zk"', '')); -- { serverError BAD_ARGUMENTS }
-- `ATTACH/REPLACE PARTITION ... FROM` without the source table.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t ATTACH PARTITION 1 FROM src'), ',"from_table":"src"', '')); -- { serverError BAD_ARGUMENTS }
-- `UNFREEZE PARTITION` without the `WITH NAME` backup name.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t UNFREEZE PARTITION 1 WITH NAME \'b\''), ',"with_name":"b"', '')); -- { serverError BAD_ARGUMENTS }

-- `cte_aliases` must be an ExpressionList of identifiers, like the column `aliases`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('WITH cte1(zzz) AS (SELECT 1) SELECT 1 FROM cte1'), '"type":"Identifier","name":"zzz"', '"type":"ExpressionList","name":"zzz"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('WITH cte1(zzz) AS (SELECT 1) SELECT 1 FROM cte1'), '"cte_aliases":{"type":"ExpressionList"', '"cte_aliases":{"type":"Identifier","name":"zzz_fake"')); -- { serverError BAD_ARGUMENTS }
