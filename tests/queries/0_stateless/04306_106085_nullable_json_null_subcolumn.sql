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

-- The precedence rewrite in `IDataType::getSubcolumnData` is restricted to direct
-- dynamic carriers (`JSON` / `Dynamic`) wrapped by `Nullable`. The type system also
-- rejects `Nullable(Array(JSON))` / `Nullable(Map(K, JSON))` at construction time,
-- so no SQL-reachable column can hit the over-broad branch; assert this here so
-- the narrow contract stays meaningful if the type rules change.
CREATE TABLE t_106085_reject_array (data Nullable(Array(JSON))) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_106085_reject_map (data Nullable(Map(String, JSON))) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The `forEachSubcolumn` skip applies only when the substream name is literally
-- `"null"` (the outer Nullable null-map). A typed-path null-map inside `JSON`
-- is named `"<path>.null"` and must still enumerate as a regular subcolumn,
-- otherwise lookup (`tryGetSubcolumnType` -> `getSubcolumnData`) and enumeration
-- (`forEachSubcolumn` -> `DESCRIBE TABLE ... describe_include_subcolumns = 1`,
-- `system.columns`) diverge for the same name.
DROP TABLE IF EXISTS t_106085_typed;
CREATE TABLE t_106085_typed (id UInt32, data Nullable(JSON(c Nullable(UInt32)))) ENGINE = Memory;
INSERT INTO t_106085_typed VALUES (1, '{"c": 42}'), (2, '{"c": null}'), (3, NULL);

-- Lookup: `data.c` is the typed `Nullable(UInt32)` path; `data.c.null` is its
-- inner UInt8 null-map (1 when the typed path value is null OR when the outer
-- Nullable row is null, 0 when the typed path holds a value).
SELECT 'typed_path_c:', id, data.c, toTypeName(data.c) FROM t_106085_typed ORDER BY id;
SELECT 'typed_path_c_null:', id, data.c.null, toTypeName(data.c.null) FROM t_106085_typed ORDER BY id;

-- Enumeration: the typed-path null-map must show up in `DESCRIBE TABLE ...
-- describe_include_subcolumns = 1` next to the typed path itself. Without the
-- `name == "null"` predicate the enumeration would also skip `c.null` (dynamic
-- JSON path can resolve it), so it would be absent from the description while
-- the lookup still finds it.
SELECT 'describe_typed:';
DESCRIBE TABLE t_106085_typed SETTINGS describe_include_subcolumns = 1 FORMAT TSVRaw;

DROP TABLE t_106085;
DROP TABLE t_106085_plain;
DROP TABLE t_106085_int;
DROP TABLE t_106085_typed;
