-- CAST converts between a map and an array of key-value pairs, and a literal written in a VALUES
-- clause, in an aggregate function parameter or in a column type name has to convert the same way.
-- Otherwise a state type name the server itself prints for a map parameter cannot be declared again,
-- and a table holding such a state cannot be attached after a restart.

DROP TABLE IF EXISTS t_map_05243;
DROP TABLE IF EXISTS t_arr_of_pairs_05243;
DROP TABLE IF EXISTS t_nested_map_05243;
DROP TABLE IF EXISTS t_state_05243;
DROP TABLE IF EXISTS t_persisted_05243;
DROP TABLE IF EXISTS t_tuple_05243;
DROP TABLE IF EXISTS t_arr_uint_05243;
DROP TABLE IF EXISTS t_src_arr_05243;
DROP TABLE IF EXISTS t_src_pairs_05243;
DROP TABLE IF EXISTS t_src_enum_05243;
DROP TABLE IF EXISTS t_src_nullable_05243;
DROP TABLE IF EXISTS t_src_nullable_arr_05243;

-- an array of pairs written into a map column, next to the CAST of the same array
CREATE TABLE t_map_05243 (m Map(String, Decimal(9, 1))) ENGINE = Memory;
INSERT INTO t_map_05243 VALUES ([('a', 1.5)]);
SELECT m, m = CAST([('a', toDecimal32(1.5, 1))], 'Map(String, Decimal(9, 1))') FROM t_map_05243;

-- an empty array is an empty map, and duplicate keys are kept, as in CAST
INSERT INTO t_map_05243 VALUES ([]), ([('b', 1.5), ('b', 2.5)]);
SELECT m FROM t_map_05243 ORDER BY length(m), m;

-- a map parameter spelled as an array of pairs
SELECT groupArrayInsertAt([('a', toDecimal32(1.5, 1))], 3)(m, i)
FROM (SELECT map('a', toDecimal32(2.5, 1)) AS m, toUInt32(2) AS i);

-- the state type name the server prints for a map parameter is accepted as a column type
CREATE TABLE t_state_05243 (s AggregateFunction(groupArrayInsertAt([], 3), Map(String, Decimal(9, 1)), UInt32))
ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_state_05243';

-- and a table holding such a state stays readable after being detached and attached. The parameter
-- is written the way it is printed, as an array of pairs: a Replicated database re-parses the
-- structure it stores, and a parameter that reads back as an Array is not interchangeable with the
-- Map a map() literal builds.
CREATE TABLE t_persisted_05243 ENGINE = MergeTree ORDER BY tuple() AS
SELECT groupArrayInsertAtState([], 3)(m, i) AS s
FROM (SELECT map('a', toDecimal32(2.5, 1)) AS m, toUInt32(0) AS i);
DETACH TABLE t_persisted_05243;
ATTACH TABLE t_persisted_05243;
SELECT finalizeAggregation(s) FROM t_persisted_05243;

-- the same conversion one level down
CREATE TABLE t_nested_map_05243 (a Array(Map(String, UInt8))) ENGINE = Memory;
INSERT INTO t_nested_map_05243 VALUES ([[('a', 1)]]);
SELECT a FROM t_nested_map_05243;

-- and in the opposite direction, map to array of pairs
CREATE TABLE t_arr_of_pairs_05243 (a Array(Tuple(String, UInt8))) ENGINE = Memory;
INSERT INTO t_arr_of_pairs_05243 VALUES (map('a', 1));
SELECT a, a = CAST(map('a', toUInt8(1)), 'Array(Tuple(String, UInt8))') FROM t_arr_of_pairs_05243;
SELECT groupArrayInsertAt(map('a', 1), 3)(a, i)
FROM (SELECT CAST([('b', 2)], 'Array(Tuple(String, UInt8))') AS a, toUInt32(2) AS i);

-- a key or a value nested in the converted collection also converts from its own source type, the
-- way CAST does: a Date is the day boundary rather than a second past the epoch, and an Enum is its
-- label rather than its numeric code.
CREATE TABLE t_src_arr_05243 (a Array(Tuple(DateTime, UInt8))) ENGINE = Memory;
INSERT INTO t_src_arr_05243 VALUES (map(toDate('1970-01-02'), 1));
SELECT a = CAST(map(toDate('1970-01-02'), toUInt8(1)), 'Array(Tuple(DateTime, UInt8))') FROM t_src_arr_05243;

-- An array literal in a VALUES clause is converted by an expression template at the column level,
-- which already matched CAST, so the two spellings below only reach the value conversion with the
-- templates turned off.
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_src_pairs_05243 (m Map(DateTime, UInt8)) ENGINE = Memory;
INSERT INTO t_src_pairs_05243 VALUES ([(toDate('1970-01-02'), 1)]);
SELECT m = CAST([(toDate('1970-01-02'), toUInt8(1))], 'Map(DateTime, UInt8)') FROM t_src_pairs_05243;

CREATE TABLE t_src_enum_05243 (a Array(Tuple(String, UInt8))) ENGINE = Memory;
INSERT INTO t_src_enum_05243 VALUES (map(CAST('a', 'Enum8(''a'' = 1)'), 1));
SELECT a = CAST(map(CAST('a', 'Enum8(''a'' = 1)'), toUInt8(1)), 'Array(Tuple(String, UInt8))') FROM t_src_enum_05243;

-- a Nullable source is stored in the same way as the value it wraps, so the nested type has to be
-- unwrapped before it can name the domain to convert from
CREATE TABLE t_src_nullable_05243 (m Map(DateTime, UInt8)) ENGINE = Memory;
INSERT INTO t_src_nullable_05243 VALUES ([(CAST(toDate('1970-01-02'), 'Nullable(Date)'), 1)]);
SELECT m = CAST([(CAST(toDate('1970-01-02'), 'Nullable(Date)'), toUInt8(1))], 'Map(DateTime, UInt8)') FROM t_src_nullable_05243;

CREATE TABLE t_src_nullable_arr_05243 (a Array(Tuple(String, DateTime))) ENGINE = Memory;
INSERT INTO t_src_nullable_arr_05243 VALUES (map('a', CAST(toDate('1970-01-02'), 'Nullable(Date)')));
SELECT a = CAST(map('a', CAST(toDate('1970-01-02'), 'Nullable(Date)')), 'Array(Tuple(String, DateTime))') FROM t_src_nullable_arr_05243;
SET input_format_values_deduce_templates_of_expressions = 1;

-- shapes CAST rejects stay rejected.
-- Which side converts an inline VALUES block decides whether the rejection is a client or a
-- server error, so pin the two settings that would move the conversion to the server:
-- async_insert is on by default, and send_table_structure_on_insert_with_inline_data is randomized.
SET async_insert = 0, send_table_structure_on_insert_with_inline_data = 1;
INSERT INTO t_map_05243 VALUES ([('a', 1, 2)]); -- { clientError TYPE_MISMATCH }
INSERT INTO t_map_05243 VALUES ([1, 2]); -- { clientError TYPE_MISMATCH }
INSERT INTO t_arr_of_pairs_05243 VALUES ([1, 2]); -- { clientError TYPE_MISMATCH }

CREATE TABLE t_arr_uint_05243 (a Array(UInt8)) ENGINE = Memory;
INSERT INTO t_arr_uint_05243 VALUES (map('a', 1)); -- { clientError TYPE_MISMATCH }

CREATE TABLE t_tuple_05243 (t Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO t_tuple_05243 VALUES ([1, 2]); -- { clientError TYPE_MISMATCH }

-- an array parameter still round-trips unchanged
SELECT toTypeName(sumMapFilteredState([1, 2])(k, v)) FROM (SELECT [1] AS k, [toUInt32(5)] AS v);

DROP TABLE t_map_05243;
DROP TABLE t_arr_of_pairs_05243;
DROP TABLE t_nested_map_05243;
DROP TABLE t_state_05243;
DROP TABLE t_persisted_05243;
DROP TABLE t_tuple_05243;
DROP TABLE t_arr_uint_05243;
DROP TABLE t_src_arr_05243;
DROP TABLE t_src_pairs_05243;
DROP TABLE t_src_enum_05243;
DROP TABLE t_src_nullable_05243;
DROP TABLE t_src_nullable_arr_05243;
