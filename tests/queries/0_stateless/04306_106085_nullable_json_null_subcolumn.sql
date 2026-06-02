-- Tags: no-fasttest
-- ^ JSON data type is not enabled in fast-test image.
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106085 :
-- reading the `.null` subcolumn from a `Nullable(JSON)` column returned the
-- outer Nullable's null-map byte instead of the value at the JSON key `null`.
-- The outer null-map of `Nullable(T)` registers a substream literally named
-- `null`, which shadows any `null` key inside the inner type `T` when `T` has
-- dynamic subcolumns (`JSON`). The fix is in `IDataType::getSubcolumnData`:
-- when `data.type` is `Nullable` and the inner type has dynamic subcolumns,
-- try the inner type's dynamic resolution first. The outer null-map remains
-- reachable via `isNull(data)` / `data IS NULL`.

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_106085;
CREATE TABLE t_106085 (id UInt32, data Nullable(JSON)) ENGINE = Memory;
INSERT INTO t_106085 VALUES (1, '{"null":"xx"}'), (2, '{"foo":"bar","null":"yy"}'), (3, NULL);

-- Bug case: `.null` previously returned the outer null-map byte (0 / UInt8).
-- After the fix it returns the JSON value at key `null` wrapped in `Nullable`,
-- and NULL for rows where the outer Nullable is NULL.
SELECT 'nullable_json_null:', id, data.null, toTypeName(data.null) FROM t_106085 ORDER BY id;

-- Other keys must keep working as before.
SELECT 'nullable_json_foo:', id, data.foo, toTypeName(data.foo) FROM t_106085 ORDER BY id;

-- Outer-row NULL state is still reachable via the standard ClickHouse pattern.
SELECT 'isNull:', id, isNull(data), data IS NULL FROM t_106085 ORDER BY id;

-- Sanity: plain `JSON` (no `Nullable` wrapper) was never affected by the bug.
DROP TABLE IF EXISTS t_106085_plain;
CREATE TABLE t_106085_plain (data JSON) ENGINE = Memory;
INSERT INTO t_106085_plain VALUES ('{"null":"xx"}');
SELECT 'plain_json_null:', data.null, toTypeName(data.null) FROM t_106085_plain;

-- Sanity: `Nullable(T)` for non-dynamic `T` must keep returning the null-map
-- under `.null` (e.g. `Nullable(Int32).null` is the UInt8 null-map column).
DROP TABLE IF EXISTS t_106085_int;
CREATE TABLE t_106085_int (data Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_106085_int VALUES (NULL), (42);
SELECT 'nullable_int_null:', data, data.null, toTypeName(data.null) FROM t_106085_int ORDER BY data NULLS FIRST SETTINGS allow_suspicious_types_in_order_by = 0;

DROP TABLE t_106085;
DROP TABLE t_106085_plain;
DROP TABLE t_106085_int;
