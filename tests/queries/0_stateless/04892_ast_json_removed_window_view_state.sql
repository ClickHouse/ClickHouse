-- Regression test for the `clickhouse_json` AST boundary after `WINDOW VIEW` was removed.
-- `ASTCreateQuery::readJSON` no longer reads `is_window_view`, the watermark strategies,
-- `allowed_lateness`, or the `watermark_function` / `lateness_function` children, and `JSONObjectReader`
-- ignores keys it does not read. Without an explicit check, a legacy payload of a window view would
-- deserialize as an ordinary `CREATE TABLE ... AS SELECT`: all create-kind flags false with `select` set,
-- which `isCreateQueryWithImmediateInsertSelect` accepts and executes as an immediate-population table
-- create - a different query than the JSON describes. Such payloads are rejected instead.

-- The payload without any window-view state round-trips unchanged (must NOT be rejected):
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE wv (cnt UInt64) ENGINE = Memory AS SELECT count(a) AS cnt FROM mt GROUP BY tumble(ts, INTERVAL 5 SECOND) AS wid'));

-- A legacy payload of an ordinary `CREATE` query carries the removed flags with `false`, because
-- `writeBool` always wrote the key. Those must still be accepted, otherwise every payload produced by
-- an older server would be rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_window_view":false,"is_watermark_strictly_ascending":false,"is_watermark_ascending":false,"is_watermark_bounded":false,"allowed_lateness":false,"is_dictionary":false'));

-- `is_window_view` set turns the payload into a window view that no longer exists:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE wv (cnt UInt64) ENGINE = Memory AS SELECT count(a) AS cnt FROM mt GROUP BY tumble(ts, INTERVAL 5 SECOND) AS wid'), '"is_dictionary":false', '"is_window_view":true,"is_dictionary":false')); -- { serverError BAD_ARGUMENTS }

-- Each watermark strategy and `ALLOWED_LATENESS` is rejected on its own, even without `is_window_view`
-- (the parser only ever attached them to a window view):
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_watermark_strictly_ascending":true,"is_dictionary":false')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_watermark_ascending":true,"is_dictionary":false')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_watermark_bounded":true,"is_dictionary":false')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"allowed_lateness":true,"is_dictionary":false')); -- { serverError BAD_ARGUMENTS }

-- The watermark / lateness expressions are rejected by their presence. They are never parsed, so the
-- placeholder body below is irrelevant - what matters is that the key is not silently dropped:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_dictionary":false,"watermark_function":{"type":"Function","name":"toIntervalSecond"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = Memory'), '"is_dictionary":false', '"is_dictionary":false,"lateness_function":{"type":"Function","name":"toIntervalSecond"}')); -- { serverError BAD_ARGUMENTS }

-- `ViewTarget::Inner` is kept only so that the UUID mappings of old DDL log entries still parse; no
-- `CREATE` syntax produces an inner target since `WINDOW VIEW` (the only form with `INNER ENGINE`) was
-- removed. A `To` target still round-trips:
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));

-- An `Inner` target carrying an engine would format as `INNER ENGINE ...`, which the SQL parser no longer
-- accepts; a bare `Inner` target would be dropped from the formatted SQL entirely. Both are rejected:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'), '"targets":[', '"targets":[{"kind":"Inner","inner_engine":{"type":"Storage","engine":{"type":"Function","name":"Memory","no_empty_args":true,"kind":"TABLE_ENGINE"}}},')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'), '"targets":[', '"targets":[{"kind":"Inner"},')); -- { serverError BAD_ARGUMENTS }
