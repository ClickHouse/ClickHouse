-- `has` compares its array elements with the key values as raw `Field`s, and the raw `Field` of a
-- `Dynamic`, `Variant` or `JSON` value does not say which type that value has, while a cast into the
-- key space does honour it. Two values that compare equal under `has` therefore sit at different key
-- values, so a set atom built for such a key side names a key value that no matching row holds and
-- prunes the granule holding it. That must not happen, neither when the key expression makes the atom
-- relaxed nor when both sides are declared alike.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_has_dynamic_cast_key;

CREATE TABLE t_has_dynamic_cast_key (d Dynamic) ENGINE = MergeTree ORDER BY d::String
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_dynamic_cast_key VALUES (true), (toDate('1970-01-02')), (toInt64(7)), ('zzz');

SELECT dynamicType(d), has([1], d) FROM t_has_dynamic_cast_key ORDER BY ALL;
SELECT sum(has([1], d)) FROM t_has_dynamic_cast_key;
SELECT count() FROM t_has_dynamic_cast_key WHERE has([1], d);
SELECT count() FROM t_has_dynamic_cast_key WHERE has([1], d) SETTINGS use_primary_key = 0;
SELECT count() FROM t_has_dynamic_cast_key WHERE NOT has([1], d);

DROP TABLE t_has_dynamic_cast_key;

-- A `Dynamic` inside a container, the two sides declared differently. Children of a container are
-- compared with the plain `Field::operator ==`, which reads the carrier before the value, so a pair
-- that matches at runtime is one sharing a carrier while holding different types: a `Date` and a
-- `UInt16` holding 1 are both carried as `UInt64` and cast to `'1970-01-02'` and `'1'`. The row that
-- holds neither sorts between the matching row and the key value the set names.

DROP TABLE IF EXISTS t_has_dynamic_in_tuple;

CREATE TABLE t_has_dynamic_in_tuple (x Tuple(UInt8, Dynamic)) ENGINE = MergeTree ORDER BY x::String
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_dynamic_in_tuple VALUES ((1, toDate('1970-01-02'))), ((1, toDate('1971-01-01'))), ((2, 'zzz'));

SELECT sum(has([(1, toUInt16(1))], x)) FROM t_has_dynamic_in_tuple;
SELECT count() FROM t_has_dynamic_in_tuple WHERE has([(1, toUInt16(1))], x);
SELECT count() FROM t_has_dynamic_in_tuple WHERE has([(1, toUInt16(1))], x) SETTINGS use_primary_key = 0;

DROP TABLE t_has_dynamic_in_tuple;

-- The same with both sides declared alike, under an injective key expression: the atom is then exact,
-- so the negative direction reports rows that do not satisfy the predicate as well.

DROP TABLE IF EXISTS t_has_dynamic_same_type;

CREATE TABLE t_has_dynamic_same_type (x Tuple(UInt8, Dynamic)) ENGINE = MergeTree ORDER BY toString(x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_dynamic_same_type VALUES ((1, toDate('1970-01-02'))), ((1, toDate('1971-01-01'))), ((2, 'zzz'));

SELECT sum(has([CAST((1, toUInt16(1)), 'Tuple(UInt8, Dynamic)')], x)) FROM t_has_dynamic_same_type;
SELECT count() FROM t_has_dynamic_same_type WHERE has([CAST((1, toUInt16(1)), 'Tuple(UInt8, Dynamic)')], x);
SELECT count() FROM t_has_dynamic_same_type
WHERE has([CAST((1, toUInt16(1)), 'Tuple(UInt8, Dynamic)')], x) SETTINGS use_primary_key = 0;
SELECT count() FROM t_has_dynamic_same_type WHERE NOT has([CAST((1, toUInt16(1)), 'Tuple(UInt8, Dynamic)')], x);
SELECT count() FROM t_has_dynamic_same_type
WHERE NOT has([CAST((1, toUInt16(1)), 'Tuple(UInt8, Dynamic)')], x) SETTINGS use_primary_key = 0;

DROP TABLE t_has_dynamic_same_type;

-- A `Variant` key side, both sides declared alike and compared at the top level, where the comparison is
-- accurate: `Date '1970-01-02'` is day 1, equal to `UInt8` 1, while the two cast to '1970-01-02' and '1'.
-- The set must occupy both alternatives, or its element narrows to one and is no longer the key's type.

DROP TABLE IF EXISTS t_has_variant_same_type;

CREATE TABLE t_has_variant_same_type (v Variant(UInt8, Date)) ENGINE = MergeTree ORDER BY toString(v)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_variant_same_type VALUES (toDate('1970-01-02')), (toDate('1971-01-01')), (7);

SELECT sum(has([CAST(1, 'Variant(UInt8, Date)'), CAST(toDate('1999-12-31'), 'Variant(UInt8, Date)')], v))
FROM t_has_variant_same_type;
SELECT count() FROM t_has_variant_same_type
WHERE has([CAST(1, 'Variant(UInt8, Date)'), CAST(toDate('1999-12-31'), 'Variant(UInt8, Date)')], v);
SELECT count() FROM t_has_variant_same_type
WHERE has([CAST(1, 'Variant(UInt8, Date)'), CAST(toDate('1999-12-31'), 'Variant(UInt8, Date)')], v)
SETTINGS use_primary_key = 0;

DROP TABLE t_has_variant_same_type;
