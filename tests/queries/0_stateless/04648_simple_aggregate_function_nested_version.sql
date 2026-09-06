-- Tags: log-engine, memory-engine
-- log-engine, memory-engine: everything here is asserted through `StripeLog`, `Memory`, `Set` and `Join`, the
-- engines that persist `Native` at revision 0. Replacing them with `MergeTree` would leave nothing to test.

-- { echo }

-- A `SimpleAggregateFunction` whose value type contains a versioned `AggregateFunction` can advertise a
-- version in the `Native` type header that the payload was not written at, so the writer cannot read back
-- its own output. `Decimal32` values are essential: `sumMap` v1 promotes them to `Decimal128`, so v0 and v1
-- differ in width and the mismatch is observable rather than byte-compatible.
--
-- The regression arms are the ones declaring an explicit `AggregateFunction(0, ...)`: a declaration that
-- pins no version is stored with the function's default spelled out, so its header and payload agree on
-- their own. The remaining wrapper, engine and parameter cases are round-trip coverage for the header
-- renderer, not assertions that fail without it.

DROP TABLE IF EXISTS sl_saf;
DROP TABLE IF EXISTS sl_tuple_value;
DROP TABLE IF EXISTS sl_array_value;
DROP TABLE IF EXISTS sl_param_value;
DROP TABLE IF EXISTS sl_map_value;
DROP TABLE IF EXISTS sl_map_key;
DROP TABLE IF EXISTS sl_named_tuple_value;
DROP TABLE IF EXISTS sl_nullable_value;
DROP TABLE IF EXISTS sl_bitmap_value;
DROP TABLE IF EXISTS sl_tuple_column;
DROP TABLE IF EXISTS sl_plain;
DROP TABLE IF EXISTS sl_sum;
DROP TABLE IF EXISTS sl_uint64;
DROP TABLE IF EXISTS mem_saf;
DROP TABLE IF EXISTS mem_saf_restored;
DROP TABLE IF EXISTS set_saf;
DROP TABLE IF EXISTS join_saf;
DROP TABLE IF EXISTS amt_saf;

-- `StripeLog` persists through `NativeWriter` at revision 0 WITH an index, so it exercises both the stream
-- header and the index header. This used to fail with `CANNOT_READ_ALL_DATA` right after the `INSERT`.
CREATE TABLE sl_saf (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = StripeLog;
INSERT INTO sl_saf SELECT 1, CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
SELECT sumMapMerge(CAST(v, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_saf;

-- An explicitly version-0 declaration is broken on master for the mirror-image reason: version 0 is never
-- printed, so the omitted version comes back as the function's default. The header must spell out the 0.
DROP TABLE IF EXISTS sl_saf_v0;
CREATE TABLE sl_saf_v0 (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = StripeLog;
INSERT INTO sl_saf_v0 SELECT 1, CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt64), Array(Decimal32(2))))');
SELECT sumMapMerge(CAST(v, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_saf_v0;
DROP TABLE sl_saf_v0;

-- The versioned leaf may sit under a wrapper inside the value type.
CREATE TABLE sl_tuple_value (k UInt8, v SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_tuple_value SELECT 1, CAST(tuple(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])), 'SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT sumMapMerge(CAST(v.1, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_tuple_value;

CREATE TABLE sl_array_value (k UInt8, v SimpleAggregateFunction(groupArrayArray, Array(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_array_value SELECT 1, CAST([sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])], 'SimpleAggregateFunction(groupArrayArray, Array(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT length(v) FROM sl_array_value;
SELECT sumMapMerge(CAST(arrayJoin(v), 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_array_value;

-- The outer function's parameters are part of the rendered header too, and a dropped parameter is not
-- cosmetic: `groupArrayLastArray` without one does not resolve at all, so the reader cannot rebuild the
-- value type. The value type is the same `Array(AggregateFunction(sumMap, ...))` carrier as above.
CREATE TABLE sl_param_value (k UInt8, v SimpleAggregateFunction(groupArrayLastArray(5), Array(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_param_value SELECT 1, CAST([sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])], 'SimpleAggregateFunction(groupArrayLastArray(5), Array(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT length(v) FROM sl_param_value;
SELECT sumMapMerge(CAST(arrayJoin(v), 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_param_value;

CREATE TABLE sl_map_value (k UInt8, v SimpleAggregateFunction(anyLast, Map(UInt64, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_map_value SELECT 1, CAST(map(7::UInt64, sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])), 'SimpleAggregateFunction(anyLast, Map(UInt64, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT sumMapMerge(CAST(v[7], 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_map_value;

-- A `Map` key is walked by a recursion of its own, independent of the value's
-- (`transformTypesRecursively.cpp` descends the key list and the value list separately), so a
-- versioned leaf in the key position needs its own assertion: dropping the renderer's key
-- recursion leaves the value case above green.
CREATE TABLE sl_map_key (k UInt8, v SimpleAggregateFunction(anyLast, Map(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))), UInt8))) ENGINE = StripeLog;
INSERT INTO sl_map_key SELECT 1, CAST(map(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 7::UInt8), 'SimpleAggregateFunction(anyLast, Map(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))), UInt8))');
SELECT length(mapKeys(v)) FROM sl_map_key;
SELECT sumMapMerge(CAST(mapKeys(v)[1], 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_map_key;

-- A named tuple must keep its element names in the rendered header, which a value round trip alone
-- cannot observe. The element name needs backquoting, so the header also loses the name outright if the
-- renderer emits it raw: an unquoted `x.y` does not parse back as one identifier.
CREATE TABLE sl_named_tuple_value (k UInt8, v SimpleAggregateFunction(anyLast, Tuple(`x.y` AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_named_tuple_value SELECT 1, CAST(tuple(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])), 'SimpleAggregateFunction(anyLast, Tuple(`x.y` AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT sumMapMerge(CAST(tupleElement(v, 'x.y'), 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_named_tuple_value;
SELECT toTypeName(v) FROM sl_named_tuple_value;
-- Reading a written `Native` file back through `file()` takes the type from the header rather than from any
-- stored declaration, so this is where a header that lost the element name becomes observable: the
-- element would come back positional and the accessor on x.y would no longer resolve.
-- The accessor is spelled `tupleElement` because the old analyzer resolves a dotted subcolumn only
-- when the storage reports `supportsSubcolumns`, which `StripeLog` does not; `tupleElement` resolves
-- by name on any storage, so the name assertion holds under both analyzers.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_04648_named_tuple.native'), Native) SELECT v FROM sl_named_tuple_value SETTINGS engine_file_truncate_on_insert = 1;
SELECT toTypeName(v) FROM file(concat(currentDatabase(), '_04648_named_tuple.native'), Native);
SELECT sumMapMerge(CAST(tupleElement(v, 'x.y'), 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM file(concat(currentDatabase(), '_04648_named_tuple.native'), Native);

-- An `AggregateFunction` cannot sit directly inside a `Nullable`, but a `Tuple` can, so a `Nullable` can still
-- appear on the path down to the versioned leaf. The version walker descends through it, so the header
-- renderer has to as well.
CREATE TABLE sl_nullable_value (k UInt8, v SimpleAggregateFunction(anyLast, Tuple(n Nullable(Tuple(x AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))))) ENGINE = StripeLog SETTINGS enable_nullable_tuple_type = 1;
INSERT INTO sl_nullable_value SELECT 1, CAST(tuple(tuple(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]))), 'SimpleAggregateFunction(anyLast, Tuple(n Nullable(Tuple(x AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))))') SETTINGS enable_nullable_tuple_type = 1;
SELECT sumMapMerge(CAST(tupleElement(tupleElement(v, 'n'), 'x'), 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_nullable_value SETTINGS enable_nullable_tuple_type = 1;

-- A second versioned function, whose versions differ by a leading `init` byte rather than by a value
-- width: `groupBitmapAnd` v1 prepends that byte and v0 does not, so a header advertising 1 over a
-- version-0 payload makes the reader consume the first payload byte as `init` and run out of data.
-- `groupBitmapAnd` is not a whitelisted `SimpleAggregateFunction` function, but the whitelist only ever
-- sees the outer `anyLast`, so it is still reachable as the value type.
CREATE TABLE sl_bitmap_value (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(groupBitmapAnd, AggregateFunction(groupBitmap, UInt64)))) ENGINE = StripeLog;
INSERT INTO sl_bitmap_value SELECT 1, CAST(s, 'SimpleAggregateFunction(anyLast, AggregateFunction(groupBitmapAnd, AggregateFunction(groupBitmap, UInt64)))') FROM (SELECT groupBitmapAndState(z) AS s FROM (SELECT groupBitmapState(u) AS z FROM (SELECT 42::UInt64 AS u)));
-- Past the `init` byte a version-1 state is exactly an unversioned `AggregateFunction(groupBitmap)`
-- state, so reinterpreting the remainder that way reads the retained element back. It still catches a
-- payload written at the other version: a version-0 payload loses its first bitmap byte to the skip and
-- fails to parse.
SELECT arraySort(bitmapToArray(CAST(substring(CAST(v AS String), 2) AS AggregateFunction(groupBitmap, UInt64)))) FROM sl_bitmap_value;

-- ... and the `SimpleAggregateFunction` itself may sit inside a container column type.
CREATE TABLE sl_tuple_column (k UInt8, v Tuple(SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_tuple_column SELECT 1, tuple(CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))'));
SELECT sumMapMerge(CAST(v.1, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM sl_tuple_column;

-- The other three engines that also persist `Native` at revision 0.
-- `Memory` holds blocks in RAM, so only `BACKUP`/`RESTORE` runs them through the `Native` writer and reader.
CREATE TABLE mem_saf (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = Memory;
INSERT INTO mem_saf SELECT 1, CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
BACKUP TABLE mem_saf TO Memory('backup_04648') FORMAT Null;
RESTORE TABLE mem_saf AS mem_saf_restored FROM Memory('backup_04648') FORMAT Null;
SELECT sumMapMerge(CAST(v, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM mem_saf_restored;

CREATE TABLE set_saf (v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = Set;
INSERT INTO set_saf SELECT CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
-- `Set` and `Join` only read their persisted `Native` file back when the table is reloaded, so an in-process
-- query alone would not exercise the writer's output at all.
DETACH TABLE set_saf;
ATTACH TABLE set_saf;
-- A `Set` restores by reading its file block by block, and a file that holds no blocks at all restores
-- silently, so the reloaded cardinality is what proves the persisted state really came back.
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'set_saf';

CREATE TABLE join_saf (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_saf SELECT 1, CAST(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
DETACH TABLE join_saf;
ATTACH TABLE join_saf;
SELECT sumMapMerge(CAST(join_saf.v, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM (SELECT 1::UInt8 AS k) AS l ANY LEFT JOIN join_saf USING (k);

-- An `AggregatingMergeTree` merge must still recognise the column through its customization.
CREATE TABLE amt_saf (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO amt_saf SELECT 1, CAST(sumMapState([1::UInt64], [1.5::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
INSERT INTO amt_saf SELECT 1, CAST(sumMapState([2::UInt64], [2.5::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))');
OPTIMIZE TABLE amt_saf FINAL;
SELECT count(), sumMapMerge(CAST(v, 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))')) FROM amt_saf GROUP BY k;

-- Controls that must keep working exactly as before.
CREATE TABLE sl_plain (k UInt8, v AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))) ENGINE = StripeLog;
INSERT INTO sl_plain SELECT 1, sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]);
SELECT sumMapMerge(v) FROM sl_plain;

CREATE TABLE sl_sum (k UInt8, v SimpleAggregateFunction(sum, UInt64)) ENGINE = StripeLog;
INSERT INTO sl_sum VALUES (1, 42);
SELECT v FROM sl_sum;

-- `UInt64` values make `sumMap` v0 and v1 the same width, so this shape round-tripped before the fix too.
CREATE TABLE sl_uint64 (k UInt8, v SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))) ENGINE = StripeLog;
INSERT INTO sl_uint64 SELECT 1, CAST(sumMapState([1::UInt64, 2::UInt64], [10::UInt64, 20::UInt64]), 'SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))');
SELECT sumMapMerge(CAST(v, 'AggregateFunction(sumMap, Array(UInt64), Array(UInt64))')) FROM sl_uint64;

SELECT toTypeName(sumMapSimpleState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]));
SELECT toTypeName(CAST('x', 'SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String)))'));

-- Stored metadata text must not move: these are the three declarations measured on master.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'sl_saf' AND name = 'v';
SELECT toTypeName(CAST(sumMapState([1::UInt64], [1.5::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt64), Array(Decimal32(2))))'));
SELECT toTypeName(CAST(sumMapState([1::UInt64], [1.5::Decimal32(2)]), 'SimpleAggregateFunction(anyLast, AggregateFunction(1, sumMap, Array(UInt64), Array(Decimal32(2))))'));

-- A `Nested` value type keeps its own spelling in a customization the `SimpleAggregateFunction` name replaces,
-- so it must survive a `Native` round trip as `Nested(...)` and never degrade to `Array(Tuple(...))`.
SELECT toTypeName(v) FROM (SELECT CAST([(1::UInt64, 2::UInt64)], 'SimpleAggregateFunction(groupArrayArray, Nested(a UInt64, b UInt64))') AS v) ORDER BY 1;
SELECT toTypeName(v) FROM (SELECT CAST([(1::UInt64, sumMapState([1::UInt64], [1.5::Decimal32(2)]))], 'SimpleAggregateFunction(groupArrayArray, Nested(a UInt64, b AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))') AS v) ORDER BY 1;

-- ... and the `Nested` spelling must be rendered by the writer itself, not only printed by `getName()`: this
-- shape reaches the versioned leaf THROUGH the `Nested` customization, so it fails on master and it is the
-- only assertion here that a renderer ignoring `Nested` cannot satisfy.
-- The first element's name needs backquoting, so the header also loses the name outright if the renderer
-- emits it raw: an unquoted `a.x` does not parse back as one identifier.
DROP TABLE IF EXISTS sl_nested_value;
CREATE TABLE sl_nested_value (k UInt8, v SimpleAggregateFunction(groupArrayArray, Nested(`a.x` UInt64, b AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))) ENGINE = StripeLog;
INSERT INTO sl_nested_value SELECT 1, CAST([(7::UInt64, sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]))], 'SimpleAggregateFunction(groupArrayArray, Nested(`a.x` UInt64, b AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))');
SELECT length(v) FROM sl_nested_value;
SELECT toTypeName(v) FROM sl_nested_value;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'sl_nested_value' AND name = 'v';
-- Both reads above report the stored declaration, not the wire header: `StripeLog` builds its output
-- header from the table metadata and keeps only the column from the `Native` block. Reading a written
-- `Native` file back is the only level at which the announced spelling is observable, so this is what
-- catches a renderer that degrades `Nested` to `Array(Tuple(...))` or drops the backquotes.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_04648_nested.native'), Native) SELECT v FROM sl_nested_value SETTINGS engine_file_truncate_on_insert = 1;
SELECT toTypeName(v) FROM file(concat(currentDatabase(), '_04648_nested.native'), Native);
SELECT length(v) FROM file(concat(currentDatabase(), '_04648_nested.native'), Native);
DROP TABLE sl_nested_value;

DROP TABLE sl_saf;
DROP TABLE sl_tuple_value;
DROP TABLE sl_array_value;
DROP TABLE sl_param_value;
DROP TABLE sl_map_value;
DROP TABLE sl_map_key;
DROP TABLE sl_named_tuple_value;
DROP TABLE sl_nullable_value;
DROP TABLE sl_bitmap_value;
DROP TABLE sl_tuple_column;
DROP TABLE sl_plain;
DROP TABLE sl_sum;
DROP TABLE sl_uint64;
DROP TABLE mem_saf;
DROP TABLE mem_saf_restored;
DROP TABLE set_saf;
DROP TABLE join_saf;
DROP TABLE amt_saf;
