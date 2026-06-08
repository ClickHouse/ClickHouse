-- Tags: no-fasttest
-- Test: json_schema_hints MergeTree setting (Layer 2)

SET allow_feature_tier = 1;

-- ============================================================
-- 1. Basic hint: pre-create dynamic paths for specific partition
-- ============================================================
DROP TABLE IF EXISTS t_hint;
CREATE TABLE t_hint
(
    id UInt64,
    action_type Int32,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    json_schema_hints = '{"payload": [{"when": "action_type = 4", "paths": {"battle_type": "UInt8", "win": "Bool", "damage": "UInt64"}}, {"when": "action_type = 3", "paths": {"item_id": "UInt32", "count": "Int64"}}]}';

-- Insert data matching hints
INSERT INTO t_hint FORMAT JSONEachRow
{"id": 1, "action_type": 4, "payload": {"battle_type": 10, "win": true, "damage": 12345}}
{"id": 2, "action_type": 4, "payload": {"battle_type": 5, "win": false, "damage": 67890}}

INSERT INTO t_hint FORMAT JSONEachRow
{"id": 3, "action_type": 3, "payload": {"item_id": 100, "count": 5}}
{"id": 4, "action_type": 3, "payload": {"item_id": 200, "count": 10}}

SELECT '-- 1. basic hint query';
SELECT id, payload.battle_type, payload.win, payload.damage FROM t_hint WHERE action_type = 4 ORDER BY id;
SELECT id, payload.item_id, payload.count FROM t_hint WHERE action_type = 3 ORDER BY id;

-- ============================================================
-- 2. Hint with IN expression
-- ============================================================
DROP TABLE IF EXISTS t_hint_in;
CREATE TABLE t_hint_in
(
    id UInt64,
    action_type Int32,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    json_schema_hints = '{"payload": [{"when": "action_type IN (28, 33)", "paths": {"rmb": "Int64", "order_id": "String"}}]}';

INSERT INTO t_hint_in FORMAT JSONEachRow
{"id": 1, "action_type": 28, "payload": {"rmb": 6800, "order_id": "ord_001"}}
{"id": 2, "action_type": 33, "payload": {"rmb": 1200, "order_id": "ord_002"}}
{"id": 3, "action_type": 5, "payload": {"other_field": "no hint for this"}}

SELECT '-- 2. IN expression hint';
SELECT id, payload.rmb, payload.order_id FROM t_hint_in WHERE action_type IN (28, 33) ORDER BY id;
SELECT id, payload.other_field FROM t_hint_in WHERE action_type = 5 ORDER BY id;

-- ============================================================
-- 3. No matching hint — standard Dynamic behavior
-- ============================================================
DROP TABLE IF EXISTS t_hint_nomatch;
CREATE TABLE t_hint_nomatch
(
    id UInt64,
    action_type Int32,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    json_schema_hints = '{"payload": [{"when": "action_type = 999", "paths": {"rare_field": "String"}}]}';

INSERT INTO t_hint_nomatch FORMAT JSONEachRow
{"id": 1, "action_type": 1, "payload": {"normal_field": "hello"}}

SELECT '-- 3. no matching hint';
SELECT id, payload.normal_field FROM t_hint_nomatch ORDER BY id;

-- ============================================================
-- 4. ALTER MODIFY SETTING (dynamic hint change)
-- ============================================================
DROP TABLE IF EXISTS t_hint_alter;
CREATE TABLE t_hint_alter
(
    id UInt64,
    action_type Int32,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Insert without hints
INSERT INTO t_hint_alter FORMAT JSONEachRow
{"id": 1, "action_type": 1, "payload": {"level": 60}}

-- Add hints
ALTER TABLE t_hint_alter MODIFY SETTING json_schema_hints = '{"payload": [{"when": "action_type = 1", "paths": {"level": "UInt8", "new_field": "String"}}]}';

-- Insert with hints active
INSERT INTO t_hint_alter FORMAT JSONEachRow
{"id": 2, "action_type": 1, "payload": {"level": 45, "new_field": "added_by_hint"}}

SELECT '-- 4. ALTER MODIFY SETTING';
SELECT id, payload.level, payload.new_field FROM t_hint_alter ORDER BY id;

-- ============================================================
-- 5. DETACH/ATTACH with hints
-- ============================================================
DETACH TABLE t_hint;
ATTACH TABLE t_hint;

SELECT '-- 5. after reattach';
SELECT id, payload.battle_type, payload.win, payload.damage FROM t_hint WHERE action_type = 4 ORDER BY id;

-- ============================================================
-- 6. Validation: when expression referencing non-partition-key column should fail
-- ============================================================
DROP TABLE IF EXISTS t_hint_invalid;
CREATE TABLE t_hint_invalid
(
    id UInt64,
    action_type Int32,
    other_col String,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    json_schema_hints = '{"payload": [{"when": "other_col = 1", "paths": {"f": "String"}}]}';

SELECT '-- 6. validation error';
INSERT INTO t_hint_invalid VALUES (1, 1, 'x', '{"f": "hello"}'); -- {serverError BAD_ARGUMENTS}

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE IF EXISTS t_hint;
DROP TABLE IF EXISTS t_hint_in;
DROP TABLE IF EXISTS t_hint_nomatch;
DROP TABLE IF EXISTS t_hint_alter;
DROP TABLE IF EXISTS t_hint_invalid;
