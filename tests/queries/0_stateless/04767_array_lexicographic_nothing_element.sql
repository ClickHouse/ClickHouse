-- An array comparison whose element types have no least supertype is executed element-wise. A
-- position holding a bare `Nothing` carries no value, so nothing can decide it: a `Tuple` is not a
-- `ColumnNullable` (no null map covers the member) and there is no length to tie-break. Such a pair
-- is rejected during analysis rather than reaching the element comparator, which used to abort the
-- server with `Bad cast from type DB::ColumnNothing to DB::ColumnVector<char8_t>`.
-- No literal expresses a bare scalar `Nothing`, so these need real columns.

DROP TABLE IF EXISTS t_nothing_tuple;
DROP TABLE IF EXISTS t_array_tuple;
CREATE TABLE t_nothing_tuple (a Array(Tuple(Nothing, UInt64))) ENGINE = Memory;
CREATE TABLE t_array_tuple (b Array(Tuple(Array(UInt8), Int64))) ENGINE = Memory;
INSERT INTO t_nothing_tuple VALUES ([(NULL, 7)]);
INSERT INTO t_array_tuple VALUES ([([1], -1)]);

-- Rejection happens while the return type is computed, not while the elements are compared, so even
-- asking for the declared type is refused. Without the analysis-time check the comparison would
-- report `UInt8` here and only fail later, which is exactly the divergence that aborted the server.
SELECT toTypeName(a = b) FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the equality family used to abort here
SELECT a =  b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a != b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isNotDistinctFrom(a, b) FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isDistinctFrom(a, b) FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the ordering operators were already rejected and must stay rejected
SELECT a <  b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a <= b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a >  b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a >= b FROM t_nothing_tuple, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- rejection happens during analysis, so an empty aligned prefix is rejected too and validity never
-- depends on the data
DROP TABLE IF EXISTS t_nothing_tuple_empty;
CREATE TABLE t_nothing_tuple_empty (a Array(Tuple(Nothing, UInt64))) ENGINE = Memory;
INSERT INTO t_nothing_tuple_empty VALUES ([]);
SELECT a = b FROM t_nothing_tuple_empty, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the same shape under array wrappers: rejection is depth-independent
DROP TABLE IF EXISTS t_nothing_2d;
DROP TABLE IF EXISTS t_array_2d;
CREATE TABLE t_nothing_2d (a Array(Array(Tuple(Nothing, UInt64)))) ENGINE = Memory;
CREATE TABLE t_array_2d (b Array(Array(Tuple(Array(UInt8), Int64)))) ENGINE = Memory;
INSERT INTO t_nothing_2d VALUES ([[(NULL, 7)]]);
INSERT INTO t_array_2d VALUES ([[([1], -1)]]);
SELECT a = b FROM t_nothing_2d, t_array_2d; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS t_nothing_3d;
DROP TABLE IF EXISTS t_array_3d;
CREATE TABLE t_nothing_3d (a Array(Array(Array(Tuple(Nothing, UInt64))))) ENGINE = Memory;
CREATE TABLE t_array_3d (b Array(Array(Array(Tuple(Array(UInt8), Int64))))) ENGINE = Memory;
INSERT INTO t_nothing_3d VALUES ([[[(NULL, 7)]]]);
INSERT INTO t_array_3d VALUES ([[[([1], -1)]]]);
SELECT a = b FROM t_nothing_3d, t_array_3d; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- a bare `Nothing` aligned against a `Nullable(T)`: that null map describes only the side that has
-- values, so it cannot decide the side that has none. Non-empty prefix deliberately.
DROP TABLE IF EXISTS t_nullable_tuple;
DROP TABLE IF EXISTS t_nothing_tuple_i64;
CREATE TABLE t_nullable_tuple (a Array(Tuple(Nullable(UInt64), UInt64))) ENGINE = Memory;
CREATE TABLE t_nothing_tuple_i64 (b Array(Tuple(Nothing, Int64))) ENGINE = Memory;
INSERT INTO t_nullable_tuple VALUES ([(1, 2)]);
INSERT INTO t_nothing_tuple_i64 VALUES ([(NULL, -1)]);
SELECT a = b FROM t_nullable_tuple, t_nothing_tuple_i64; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Both sides bare `Nothing` in the same aligned position, differing only in the signedness of the
-- carrying member. The element resolver answers `Nullable(UInt8)` for such a pair, but that column
-- is null for every row, so the pair still decides nothing and must be rejected. Both operand
-- orders, because a classifier that stopped at the first aligned pair would accept this.
DROP TABLE IF EXISTS t_nothing_tuple_u64_pair;
DROP TABLE IF EXISTS t_nothing_tuple_i64_pair;
CREATE TABLE t_nothing_tuple_u64_pair (a Array(Tuple(Nothing, UInt64))) ENGINE = Memory;
CREATE TABLE t_nothing_tuple_i64_pair (b Array(Tuple(Nothing, Int64))) ENGINE = Memory;
INSERT INTO t_nothing_tuple_u64_pair VALUES ([(NULL, 5)]);
INSERT INTO t_nothing_tuple_i64_pair VALUES ([(NULL, 7)]);
SELECT toTypeName(a = b) FROM t_nothing_tuple_u64_pair, t_nothing_tuple_i64_pair; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a = b FROM t_nothing_tuple_u64_pair, t_nothing_tuple_i64_pair; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT b = a FROM t_nothing_tuple_u64_pair, t_nothing_tuple_i64_pair; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a > b FROM t_nothing_tuple_u64_pair, t_nothing_tuple_i64_pair; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT b > a FROM t_nothing_tuple_u64_pair, t_nothing_tuple_i64_pair; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same mixed signed/unsigned shape without `Nothing` stays accepted, so the rows above pin the
-- `Nothing` member and not the signedness difference.
DROP TABLE IF EXISTS t_string_tuple_u64;
DROP TABLE IF EXISTS t_string_tuple_i64;
CREATE TABLE t_string_tuple_u64 (a Array(Tuple(String, UInt64))) ENGINE = Memory;
CREATE TABLE t_string_tuple_i64 (b Array(Tuple(String, Int64))) ENGINE = Memory;
INSERT INTO t_string_tuple_u64 VALUES ([('x', 5)]);
INSERT INTO t_string_tuple_i64 VALUES ([('x', 7)]);
SELECT a = b FROM t_string_tuple_u64, t_string_tuple_i64;
SELECT a > b FROM t_string_tuple_u64, t_string_tuple_i64;

-- A `Nullable(T)` wrapper carries values, so it decides nothing about a bare `Nothing` nested under
-- it: such a side must still be rejected. `Nullable(Tuple(...))` needs its own setting to exist.
SET enable_nullable_tuple_type = 1;
DROP TABLE IF EXISTS t_nullable_tuple_nothing;
CREATE TABLE t_nullable_tuple_nothing (a Array(Nullable(Tuple(Nothing, UInt64)))) ENGINE = Memory;
INSERT INTO t_nullable_tuple_nothing VALUES ([(NULL, 7)]);
SELECT toTypeName(a = b) FROM t_nullable_tuple_nothing, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a = b FROM t_nullable_tuple_nothing, t_array_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET enable_nullable_tuple_type = 0;

-- A side's own `Nullable(Nothing)` member does not excuse the other side's nested bare `Nothing`:
-- each side is classified on its own. Asserted on the declared type and in both operand orders,
-- because a pairwise classifier would stop at the first member and accept the pair here.
DROP TABLE IF EXISTS t_nullable_nothing_tuple;
DROP TABLE IF EXISTS t_nested_nothing_tuple;
CREATE TABLE t_nullable_nothing_tuple (a Array(Tuple(Nullable(Nothing), UInt64))) ENGINE = Memory;
CREATE TABLE t_nested_nothing_tuple (b Array(Tuple(Tuple(Nothing, UInt64), Int64))) ENGINE = Memory;
SELECT toTypeName(a = b) FROM t_nullable_nothing_tuple, t_nested_nothing_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(b = a) FROM t_nullable_nothing_tuple, t_nested_nothing_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT a = b FROM t_nullable_nothing_tuple, t_nested_nothing_tuple; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `Map` and `Array` element types whose value type is `Nothing` share a supertype with their
-- partner, so those positions never reach the element path and must keep answering.
DROP TABLE IF EXISTS t_map_nothing;
DROP TABLE IF EXISTS t_map_uint8;
CREATE TABLE t_map_nothing (a Array(Tuple(Map(UInt8, Nothing), UInt64))) ENGINE = Memory;
CREATE TABLE t_map_uint8 (b Array(Tuple(Map(UInt8, UInt8), Int64))) ENGINE = Memory;
INSERT INTO t_map_nothing VALUES ([(map(), 5)]);
INSERT INTO t_map_uint8 VALUES ([(map(1, 2), -1)]);
SELECT a = b FROM t_map_nothing, t_map_uint8;

DROP TABLE IF EXISTS t_nested_array_nothing;
DROP TABLE IF EXISTS t_nested_array_uint8;
CREATE TABLE t_nested_array_nothing (a Array(Tuple(Array(Tuple(Nothing, UInt8)), UInt64))) ENGINE = Memory;
CREATE TABLE t_nested_array_uint8 (b Array(Tuple(Array(Tuple(UInt8, UInt8)), Int64))) ENGINE = Memory;
INSERT INTO t_nested_array_nothing VALUES ([([], 5)]);
INSERT INTO t_nested_array_uint8 VALUES ([([(2, 1)], -1)]);
SELECT a =  b FROM t_nested_array_nothing, t_nested_array_uint8;
SELECT a != b FROM t_nested_array_nothing, t_nested_array_uint8;

-- a runtime array comparison against an untyped empty array answers instead of aborting
DROP TABLE IF EXISTS t_arrays;
CREATE TABLE t_arrays (a Array(Array(UInt8))) ENGINE = Memory;
INSERT INTO t_arrays VALUES ([[1]]), ([[2],[3]]);
SELECT count() FROM t_arrays WHERE a > CAST([], 'Array(Nullable(Nothing))');

DROP TABLE t_nothing_tuple;
DROP TABLE t_array_tuple;
DROP TABLE t_nothing_tuple_empty;
DROP TABLE t_nothing_2d;
DROP TABLE t_array_2d;
DROP TABLE t_nothing_3d;
DROP TABLE t_array_3d;
DROP TABLE t_nullable_tuple;
DROP TABLE t_nothing_tuple_i64;
DROP TABLE t_nullable_nothing_tuple;
DROP TABLE t_nested_nothing_tuple;
DROP TABLE t_nullable_tuple_nothing;
DROP TABLE t_nothing_tuple_u64_pair;
DROP TABLE t_nothing_tuple_i64_pair;
DROP TABLE t_string_tuple_u64;
DROP TABLE t_string_tuple_i64;
DROP TABLE t_map_nothing;
DROP TABLE t_map_uint8;
DROP TABLE t_nested_array_nothing;
DROP TABLE t_nested_array_uint8;
DROP TABLE t_arrays;
