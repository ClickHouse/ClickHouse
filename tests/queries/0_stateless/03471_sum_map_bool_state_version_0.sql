-- Regression test for "Cannot sum Bools" LOGICAL_ERROR.
-- Version-0 sumMap-family state used to deserialize Bool values into bool-tagged Fields,
-- which threw in FieldVisitorSum on merge when two states shared a map key.

DROP TABLE IF EXISTS t_sum_map_bool_v0;
CREATE TABLE t_sum_map_bool_v0 (s AggregateFunction(0, sumMap, Array(UInt8), Array(Bool))) ENGINE = TinyLog;
INSERT INTO t_sum_map_bool_v0 SELECT sumMapState([1], [true]);
INSERT INTO t_sum_map_bool_v0 SELECT sumMapState([1], [false]);
SELECT sumMapMerge(s) FROM t_sum_map_bool_v0;
DROP TABLE t_sum_map_bool_v0;

DROP TABLE IF EXISTS t_sum_map_of_bool_v0;
CREATE TABLE t_sum_map_of_bool_v0 (s AggregateFunction(0, sumMap, Array(UInt8), Array(Bool))) ENGINE = TinyLog;
INSERT INTO t_sum_map_of_bool_v0 SELECT sumMapState([1], [true]);
INSERT INTO t_sum_map_of_bool_v0 SELECT sumMapState([1], [true]);
SELECT sumMapMerge(s) FROM t_sum_map_of_bool_v0;
DROP TABLE t_sum_map_of_bool_v0;

DROP TABLE IF EXISTS t_sum_map_overflow_bool_v0;
CREATE TABLE t_sum_map_overflow_bool_v0 (s AggregateFunction(0, sumMapWithOverflow, Array(UInt8), Array(Bool))) ENGINE = TinyLog;
INSERT INTO t_sum_map_overflow_bool_v0 SELECT sumMapWithOverflowState([1], [true]);
INSERT INTO t_sum_map_overflow_bool_v0 SELECT sumMapWithOverflowState([1], [false]);
SELECT sumMapWithOverflowMerge(s) FROM t_sum_map_overflow_bool_v0;
DROP TABLE t_sum_map_overflow_bool_v0;

DROP TABLE IF EXISTS t_sum_map_nullable_bool_v0;
CREATE TABLE t_sum_map_nullable_bool_v0 (s AggregateFunction(0, sumMap, Array(UInt8), Array(Nullable(Bool)))) ENGINE = TinyLog;
INSERT INTO t_sum_map_nullable_bool_v0 SELECT sumMapState([1], [true::Nullable(Bool)]);
INSERT INTO t_sum_map_nullable_bool_v0 SELECT sumMapState([1], [false::Nullable(Bool)]);
SELECT sumMapMerge(s) FROM t_sum_map_nullable_bool_v0;
DROP TABLE t_sum_map_nullable_bool_v0;

-- Bool as the map KEY for a version-0 state. The key deserialize runs outside the version switch,
-- so merge a STORED (deserialized) version-0 state with a FRESH add() state: without the flag the
-- deserialized key stays bool-tagged while add() produces an int-tagged key, so they wrongly stay
-- separate. Two stored states would share the deserialized carrier and dedup even on buggy code,
-- so the mixed form is what pins the fix.
DROP TABLE IF EXISTS t_sum_map_bool_key_v0;
CREATE TABLE t_sum_map_bool_key_v0 (s AggregateFunction(0, sumMap, Array(Bool), Array(UInt32))) ENGINE = TinyLog;
INSERT INTO t_sum_map_bool_key_v0 SELECT sumMapState(CAST([true], 'Array(Bool)'), CAST([10], 'Array(UInt32)'));
SELECT sumMapMerge(s) FROM (
    SELECT s FROM t_sum_map_bool_key_v0
    UNION ALL
    SELECT sumMapState(CAST([true], 'Array(Bool)'), CAST([10], 'Array(UInt32)'))
);
DROP TABLE t_sum_map_bool_key_v0;

-- The flag is set before the version switch, so the Bool KEY deserialize also applies to the
-- current default state format (no explicit version). Pin the mixed path: a STORED (deserialized)
-- state merged with a FRESH add() state must dedup the Bool key. Without the flag the deserialized
-- key stays bool-tagged while add() produces an int-tagged key, so they wrongly stay separate.
DROP TABLE IF EXISTS t_sum_map_bool_key_default;
CREATE TABLE t_sum_map_bool_key_default (s AggregateFunction(sumMap, Array(Bool), Array(UInt32))) ENGINE = TinyLog;
INSERT INTO t_sum_map_bool_key_default SELECT sumMapState(CAST([true], 'Array(Bool)'), CAST([10], 'Array(UInt32)'));
SELECT sumMapMerge(s) FROM (
    SELECT s FROM t_sum_map_bool_key_default
    UNION ALL
    SELECT sumMapState(CAST([true], 'Array(Bool)'), CAST([10], 'Array(UInt32)'))
);
DROP TABLE t_sum_map_bool_key_default;

-- The serialization version comes from the AggregateFunction data type parameter, which is
-- user/data-controlled and not validated at type creation. Serializing a state whose type
-- carries an unknown version must throw a catchable exception, not a LOGICAL_ERROR that aborts
-- debug/sanitizer builds (found by the AST fuzzer mutating the version into AggregateFunction(256, ...)).
SELECT hex(CAST(sumMapState([1], [10::UInt32]), 'AggregateFunction(256, sumMap, Array(UInt8), Array(UInt32))')); -- { serverError BAD_ARGUMENTS }
SELECT hex(CAST(sumMapWithOverflowState([1], [10::UInt32]), 'AggregateFunction(256, sumMapWithOverflow, Array(UInt8), Array(UInt32))')); -- { serverError BAD_ARGUMENTS }
SELECT hex(CAST(minMapState([1], [10::UInt32]), 'AggregateFunction(256, minMap, Array(UInt8), Array(UInt32))')); -- { serverError BAD_ARGUMENTS }
SELECT hex(CAST(maxMapState([1], [10::UInt32]), 'AggregateFunction(256, maxMap, Array(UInt8), Array(UInt32))')); -- { serverError BAD_ARGUMENTS }
SELECT hex(CAST(sumMapFilteredState([1])([1], [10::UInt32]), 'AggregateFunction(256, sumMapFiltered([1]), Array(UInt8), Array(UInt32))')); -- { serverError BAD_ARGUMENTS }
