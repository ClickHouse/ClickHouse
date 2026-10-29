-- A Tuple-typed key column wrapped in a key transform: `k IN (<tuple literal>)` builds the set
-- unpacked, one column per tuple element, so key analysis must re-pack it against the transform's
-- input type. Master raises TYPE_MISMATCH on that layout, or silently returns a wrong count where the
-- bad cast happens to parse. Whether an atom over this layout is exact follows the transform's own
-- injectivity claim, as for any other layout. A layout that the key expression result type re-packs
-- by itself is left exactly as it was, and arms 12 and 13 hold that line.

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_float;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_nan;
DROP TABLE IF EXISTS t_dt;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_mm;
DROP TABLE IF EXISTS t_set;
DROP TABLE IF EXISTS t_id;
DROP TABLE IF EXISTS t_hex;
DROP TABLE IF EXISTS t_inj;
DROP TABLE IF EXISTS t_time;
DROP TABLE IF EXISTS t_qbit;
DROP TABLE IF EXISTS t_dt64;

-- 1. The set element is a String that itself parses as a tuple, so the bad cast used to succeed
-- with an unrelated value and prune the matching granule: silent row loss, no error.
CREATE TABLE t_str (k Tuple(String, String)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_str SELECT tuple('(\'a\',\'b\')', 'zzz');
SELECT count() FROM t_str WHERE k IN (tuple('(\'a\',\'b\')', 'zzz'));
SELECT count() FROM t_str WHERE k IN (tuple('(\'a\',\'b\')', 'zzz')) SETTINGS use_primary_key = 0;

-- 2. A multi-column subquery arrives unpacked as well. The plan for `count()` on this shape is an
-- exact-count projection that carries no index block, so the `EXPLAIN` here reads rows instead. The
-- plan nests under a coordinator node when parallel replicas are on, prefixing the line with tree
-- drawing that `trimLeft` does not strip, so all three of these asserts extract the line instead.
SELECT count() FROM t_str WHERE k IN (SELECT '(\'a\',\'b\')', 'zzz');
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_str WHERE k IN (SELECT '(\'a\',\'b\')', 'zzz')
) WHERE explain ILIKE '%Condition:%';

-- 3. Every other element type threw `CAST AS Tuple can only be performed ...` instead of answering.
CREATE TABLE t_float (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_float SELECT tuple(toFloat64(3), toFloat64(1));
INSERT INTO t_float SELECT tuple(toFloat64(5), toFloat64(1));
SELECT count() FROM t_float WHERE k IN (tuple(toFloat64(3), toFloat64(1)));

-- 4. The originating report: a `Nullable` element inside the key tuple, and a two-element set holding
-- one tuple with a `NULL` and one without. Master raises TYPE_MISMATCH; the answer is 1, and the
-- `use_primary_key = 0` run anchors that against the path index analysis does not reach.
CREATE TABLE t_null (k Tuple(Nullable(Float64), Float64)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_null SELECT tuple(CAST(NULL, 'Nullable(Float64)'), toFloat64(1));
INSERT INTO t_null SELECT tuple(CAST(3., 'Nullable(Float64)'), toFloat64(1));
SELECT count() FROM t_null WHERE k IN (tuple(CAST(NULL, 'Nullable(Float64)'), toFloat64(1)),
                                       tuple(CAST(3., 'Nullable(Float64)'), toFloat64(1)));
SELECT count() FROM t_null WHERE k IN (tuple(CAST(NULL, 'Nullable(Float64)'), toFloat64(1)),
                                       tuple(CAST(3., 'Nullable(Float64)'), toFloat64(1)))
    SETTINGS use_primary_key = 0;

-- 5. `toString` maps every NaN payload to `nan` while `IN` compares floats bitwise, so the
-- transformed set covers rows the predicate does not. Both directions are asserted because the
-- merge is unsound in opposite ways: an exact atom here answers `0` and `1`.
CREATE TABLE t_nan (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_nan SELECT tuple(reinterpretAsFloat64(toUInt64(9221120237041090561)), toFloat64(1));
SELECT count() FROM t_nan WHERE k NOT IN (tuple(reinterpretAsFloat64(toUInt64(9221120237041090562)), toFloat64(1)));
SELECT count() FROM t_nan WHERE k IN (tuple(reinterpretAsFloat64(toUInt64(9221120237041090562)), toFloat64(1)));

-- 6. `toString` of a date-time in a timezone with a fall-back transition renders two distinct
-- instants as one local time, so the layout the re-pack newly admits is not exact for it either.
-- The timezone is named in the column type and in both literals, so the arm does not depend on the
-- server timezone, which the runner randomizes. An exact atom here answers `0` and `1`.
CREATE TABLE t_dt (k Tuple(DateTime('Europe/Berlin'), UInt8)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_dt SELECT tuple(toDateTime(1698539400, 'Europe/Berlin'), 1);
SELECT count() FROM t_dt WHERE k NOT IN (tuple(toDateTime(1698543000, 'Europe/Berlin'), 1));
SELECT count() FROM t_dt WHERE k IN (tuple(toDateTime(1698543000, 'Europe/Berlin'), 1));

-- 7. Partition pruning runs whatever `use_primary_key` says, so this arm had no escape hatch. The
-- count alone would come out right even with the atom declined, since scanning both partitions also
-- counts one row; the `Condition:` line is what shows the atom was analysed and applied. The narrow
-- ILIKE picks the partition entry, because a partitioned table also prints a `PrimaryKey` one.
CREATE TABLE t_part (k Tuple(Float64, Float64)) ENGINE = MergeTree PARTITION BY toString(k) ORDER BY tuple();
INSERT INTO t_part SELECT tuple(toFloat64(3), toFloat64(1));
INSERT INTO t_part SELECT tuple(toFloat64(5), toFloat64(1));
SELECT count() FROM t_part WHERE k IN (tuple(toFloat64(3), toFloat64(1)));
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_part WHERE k IN (tuple(toFloat64(3), toFloat64(1)))
) WHERE explain ILIKE '%Condition: (toString(k) in%';

-- 8. The primary-key decline detector: this line reports `Condition: true` once the atom is
-- declined, where the arms that assert only a count still answer correctly by full scan. The
-- partition and `minmax` arms carry their own. Granule counts are not asserted because
-- randomized `index_granularity` moves them.
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_float WHERE k IN (tuple(toFloat64(3), toFloat64(1)))
) WHERE explain ILIKE '%Condition:%';

-- 9. `NOT IN` over this layout is analysed like `IN`. `toString` of a `Float64` is not injective, so
-- the atom is relaxed and does not prune on its own; the count must stay right.
SELECT count() FROM t_float WHERE k NOT IN (tuple(toFloat64(3), toFloat64(1)));
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_float WHERE k NOT IN (tuple(toFloat64(3), toFloat64(1)))
) WHERE explain ILIKE '%Condition:%';

-- 10. A `minmax` skip index over the transform is another consumer of the same key analysis.
-- `force_data_skipping_indices` raises INDEX_NOT_USED when the atom is declined, which detects that
-- without asserting a granule count.
CREATE TABLE t_mm (k Tuple(Float64, Float64), INDEX i_mm toString(k) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mm SELECT tuple(toFloat64(number), toFloat64(1)) FROM numbers(8);
SELECT count() FROM t_mm WHERE k IN (tuple(toFloat64(3), toFloat64(1)))
    SETTINGS force_data_skipping_indices = 'i_mm';

-- 11. A `set` skip index over the transform threw the same error, from building its own condition.
-- This is an answer-only assert, unlike arms 7, 8 and 10: a `set` index decides usability by column
-- name against its index expression, so a predicate on `k` is declined either way, fix or no fix.
CREATE TABLE t_set (k Tuple(Float64, Float64), INDEX i_set toString(k) TYPE set(100) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_set SELECT tuple(toFloat64(number), toFloat64(1)) FROM numbers(8);
SELECT count() FROM t_set WHERE k IN (tuple(toFloat64(3), toFloat64(1)));

-- 12. A transform whose own result type is already a tuple of the set's arity re-packs the set
-- without the input-type rule, so it is not admitted here for the first time and keeps the exactness
-- it had: `identity` returns its input unchanged, and master analyses this atom under `NOT IN`. The
-- `Condition:` line is the only oracle, since the count is right either way.
CREATE TABLE t_id (k Tuple(UInt32, UInt32)) ENGINE = MergeTree ORDER BY identity(k);
INSERT INTO t_id SELECT tuple(toUInt32(3), toUInt32(1));
INSERT INTO t_id SELECT tuple(toUInt32(5), toUInt32(1));
SELECT count() FROM t_id WHERE k NOT IN (tuple(toUInt32(3), toUInt32(1)));
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_id WHERE k NOT IN (tuple(toUInt32(3), toUInt32(1)))
) WHERE explain ILIKE '%Condition: (identity(k) notIn%';

-- 13. A scalar key never reaches the re-pack at all, so it keeps its exactness too: `hex` renders
-- every byte of the float, and master analyses this atom under `NOT IN`. The set holds two elements
-- because a one-element one is answered from an exact-count projection that prints no index block.
CREATE TABLE t_hex (k Float64) ENGINE = MergeTree ORDER BY hex(k);
INSERT INTO t_hex SELECT toFloat64(3);
INSERT INTO t_hex SELECT toFloat64(5);
SELECT count() FROM t_hex WHERE k NOT IN (toFloat64(3), toFloat64(7));
SELECT extract(explain, 'Condition:.*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_hex WHERE k NOT IN (toFloat64(3), toFloat64(7))
) WHERE explain ILIKE '%Condition: (hex(k) notIn%';

-- 14. An injective transform keeps an exact atom over this layout: `toString` keeps distinct
-- `Tuple(Int64, String)` values distinct, so `NOT IN` prunes the part holding only the excluded value.
-- Merges are stopped so each part stays one key value. `Parts:` is the exactness oracle, since a
-- relaxed atom prints the same `Condition:` line and reads both parts.
CREATE TABLE t_inj (k Tuple(Int64, String)) ENGINE = MergeTree ORDER BY toString(k);
SYSTEM STOP MERGES t_inj;
INSERT INTO t_inj SELECT tuple(toInt64(1), 'a');
INSERT INTO t_inj SELECT tuple(toInt64(2), 'b');
SELECT count() FROM t_inj WHERE k NOT IN (tuple(toInt64(1), 'a'));
SELECT extract(explain, '(?:Condition|Parts): .*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_inj WHERE k NOT IN (tuple(toInt64(1), 'a'))
) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Parts:%';

-- 15. `toString` of a `Time` beyond `999:59:59` saturates, so the two rows below render alike and the
-- atom must stay relaxed: an exact one answers `0` for `NOT IN`, and `2` for `IN` when counted from the index.
CREATE TABLE t_time (k Tuple(Time, UInt8)) ENGINE = MergeTree ORDER BY toString(k);
SYSTEM STOP MERGES t_time;
INSERT INTO t_time SELECT tuple(t, 1) FROM format(RowBinary, 't Time', unhex('80EE3600'));
INSERT INTO t_time SELECT tuple(t, 1) FROM format(RowBinary, 't Time', unhex('81EE3600'));
SELECT count() FROM t_time WHERE k IN (SELECT t, toUInt8(1) FROM format(RowBinary, 't Time', unhex('80EE3600')));
SELECT count() FROM t_time WHERE k NOT IN (SELECT t, toUInt8(1) FROM format(RowBinary, 't Time', unhex('80EE3600')));

-- 16. `toString` renders every `NaN` payload of a `QBit` element as `nan`, so the two rows below render alike and the
-- atom must stay relaxed: an exact one answers `0` for `NOT IN`, and `2` for `IN` when counted from the index.
-- Parallel replicas are off: a `QBit` constant sent to a replica arrives as a `Tuple` of `String` and fails to cast.
CREATE TABLE t_qbit (k Tuple(QBit(Float32, 1), UInt8)) ENGINE = MergeTree ORDER BY toString(k);
SYSTEM STOP MERGES t_qbit;
INSERT INTO t_qbit SELECT tuple(CAST([reinterpretAsFloat32(toUInt32(2143289345))], 'QBit(Float32, 1)'), 1);
INSERT INTO t_qbit SELECT tuple(CAST([reinterpretAsFloat32(toUInt32(2143289346))], 'QBit(Float32, 1)'), 1);
SELECT count() FROM t_qbit WHERE k IN (SELECT CAST([reinterpretAsFloat32(toUInt32(2143289345))], 'QBit(Float32, 1)'), toUInt8(1)) SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM t_qbit WHERE k NOT IN (SELECT CAST([reinterpretAsFloat32(toUInt32(2143289345))], 'QBit(Float32, 1)'), toUInt8(1)) SETTINGS enable_parallel_replicas = 0;

-- 17. The same holds for `has`, whose array elements arrive packed already: `NOT has` prunes the part
-- holding only the excluded value.
SELECT count() FROM t_inj WHERE NOT has([tuple(toInt64(1), 'a')], k) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT extract(explain, '(?:Condition|Parts): .*') FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_inj WHERE NOT has([tuple(toInt64(1), 'a')], k) SETTINGS optimize_rewrite_has_to_in = 0
) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Parts:%';

-- 18. `toString` of a `DateTime64` past `9999-12-31` saturates, so the two rows below render alike and the atom
-- must stay relaxed: an exact one answers `2` for `IN` when counted from the index, and `0` for `NOT IN` and `NOT has`.
-- Parallel replicas are off: such a constant sent to a replica is rendered as text, which saturates the same way.
CREATE TABLE t_dt64 (k Tuple(DateTime64(0, 'UTC'), UInt8)) ENGINE = MergeTree ORDER BY toString(k);
SYSTEM STOP MERGES t_dt64;
INSERT INTO t_dt64 SELECT tuple(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1, 1);
INSERT INTO t_dt64 SELECT tuple(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 2, 1);
SELECT count() FROM t_dt64 WHERE k IN (SELECT toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1, toUInt8(1)) SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM t_dt64 WHERE k NOT IN (SELECT toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1, toUInt8(1)) SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM t_dt64 WHERE NOT has([tuple(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1, toUInt8(1))], k) SETTINGS optimize_rewrite_has_to_in = 0, enable_parallel_replicas = 0;

DROP TABLE t_str;
DROP TABLE t_float;
DROP TABLE t_null;
DROP TABLE t_nan;
DROP TABLE t_dt;
DROP TABLE t_part;
DROP TABLE t_mm;
DROP TABLE t_set;
DROP TABLE t_id;
DROP TABLE t_hex;
DROP TABLE t_inj;
DROP TABLE t_time;
DROP TABLE t_qbit;
DROP TABLE t_dt64;
