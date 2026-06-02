-- Tags: no-fasttest, no-old-analyzer
-- ^ JSON data type is not enabled in fast-test image; `FunctionToSubcolumnsPass`
--   is a new-analyzer pass with no old-analyzer equivalent, so the A2 rewrite
--   assertions cannot fire under the old analyzer (EXPLAIN reads `data` directly).
--
-- Companion test for https://github.com/ClickHouse/ClickHouse/issues/106085 fix
-- in `IDataType::forEachSubcolumn`. The fix hides the static `.null` substream
-- of `Nullable(T)` when `T` has dynamic subcolumns and `T` can resolve the
-- subcolumn name dynamically (e.g. `Nullable(JSON)` where `JSON` has a key
-- named `null`). This is needed for `SELECT data.null FROM (data Nullable(JSON))`
-- to return the JSON value at key `null` rather than the outer null-map byte.
--
-- Side effect: for `Nullable(JSON)` (and other `Nullable(T-with-dynamic)`),
-- `FunctionToSubcolumnsPass` no longer rewrites `count(x)`, `isNull(x)`, and
-- `isNotNull(x)` to read the `.null` UInt8 subcolumn -- it now reads the full
-- `Nullable(JSON)` column. The result is still correct.
--
-- This test pins down both halves of the trade-off:
--   A) `Nullable(Int)` still gets the rewrite (sanity).
--   B) `Nullable(JSON)` does NOT get the rewrite (documented regression).

SET enable_json_type = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_04308_int;
CREATE TABLE t_04308_int (data Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04308_int VALUES (1), (NULL), (3);

-- A1. Result correctness (Nullable(Int)).
SELECT 'int:count', count(data) FROM t_04308_int;
SELECT 'int:isNull', sum(isNull(data)) FROM t_04308_int;
SELECT 'int:isNotNull', sum(isNotNull(data)) FROM t_04308_int;

-- A2. Rewrite is applied: `data.null` UInt8 appears in the pipeline.
SELECT 'int:count_rewritten', countIf(position(explain, 'data.null') > 0) > 0
FROM (EXPLAIN actions=1 SELECT count(data) FROM t_04308_int);
SELECT 'int:isNull_rewritten', countIf(position(explain, 'data.null') > 0) > 0
FROM (EXPLAIN actions=1 SELECT isNull(data) FROM t_04308_int);
SELECT 'int:isNotNull_rewritten', countIf(position(explain, 'data.null') > 0) > 0
FROM (EXPLAIN actions=1 SELECT isNotNull(data) FROM t_04308_int);

DROP TABLE IF EXISTS t_04308_json;
CREATE TABLE t_04308_json (data Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04308_json VALUES ('{"foo":1}'), (NULL), ('{"null":"xx"}');

-- B1. Result correctness (Nullable(JSON)) -- rewrite is gone but result is the same.
SELECT 'json:count', count(data) FROM t_04308_json;
SELECT 'json:isNull', sum(isNull(data)) FROM t_04308_json;
SELECT 'json:isNotNull', sum(isNotNull(data)) FROM t_04308_json;

-- B2. Rewrite is NOT applied: `data.null` UInt8 does not appear -- the planner
-- reads the full `Nullable(JSON)` column instead. This is the documented cost
-- of fixing the `.null` JSON-key collision in #106085.
SELECT 'json:count_not_rewritten', countIf(position(explain, 'data.null') > 0) = 0
FROM (EXPLAIN actions=1 SELECT count(data) FROM t_04308_json);
SELECT 'json:isNull_not_rewritten', countIf(position(explain, 'data.null') > 0) = 0
FROM (EXPLAIN actions=1 SELECT isNull(data) FROM t_04308_json);
SELECT 'json:isNotNull_not_rewritten', countIf(position(explain, 'data.null') > 0) = 0
FROM (EXPLAIN actions=1 SELECT isNotNull(data) FROM t_04308_json);

-- B3. The `.null` JSON-key user-visible value is preserved (the bug fix from
-- #106085 -- duplicated here so this test self-contains the trade-off summary).
SELECT 'json:data.null_returns_json_value', data.null
FROM t_04308_json
WHERE data IS NOT NULL AND data.null IS NOT NULL;

DROP TABLE t_04308_int;
DROP TABLE t_04308_json;
