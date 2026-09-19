-- A Tuple-typed key column wrapped in a key transform: `k IN (<tuple literal>)` builds the set
-- unpacked, one column per tuple element, so key analysis must re-pack it against the transform's
-- input type. Master raises TYPE_MISMATCH on that layout, or silently returns a wrong count where the
-- bad cast happens to parse. Re-packing it is not enough on its own: nothing is known about whether the
-- transform keeps distinct key values distinct, so a layout admitted here for the first time must not
-- yield an exact atom. A layout that the key expression result type re-packs by itself is left exactly
-- as it was, and the last two arms hold that line.

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
-- merge is unsound in opposite ways: without the guard the answers are `0` and `1`.
CREATE TABLE t_nan (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k);
INSERT INTO t_nan SELECT tuple(reinterpretAsFloat64(toUInt64(9221120237041090561)), toFloat64(1));
SELECT count() FROM t_nan WHERE k NOT IN (tuple(reinterpretAsFloat64(toUInt64(9221120237041090562)), toFloat64(1)));
SELECT count() FROM t_nan WHERE k IN (tuple(reinterpretAsFloat64(toUInt64(9221120237041090562)), toFloat64(1)));

-- 6. `toString` of a date-time in a timezone with a fall-back transition renders two distinct
-- instants as one local time, so the layout the re-pack newly admits is not exact for it either.
-- The timezone is named in the column type and in both literals, so the arm does not depend on the
-- server timezone, which the runner randomizes. Master throws TYPE_MISMATCH here; admitting the
-- layout without the guard answers `0` and `1`.
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

-- 9. `NOT IN` over a newly admitted layout falls back to a scan, since the transform is not known to
-- keep distinct key values distinct; the count must stay right either way. The `Condition:` line is
-- what separates that fall-back from an atom admitted as a relaxed one, which counts right as well.
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
