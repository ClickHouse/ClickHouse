-- A block of aggregate function states declares how many rows it holds, and the size of one state
-- comes from the type, so neither of them may be turned into an allocation on its own: the product
-- of the two wraps around, and a block of a hundred bytes can ask for an enormous allocation.

-- A block that declares a billion rows of an eight byte state and carries none. It must be read as
-- it arrives instead of allocating the eight gigabytes it claims.
SELECT count() FROM format(Native, 'x AggregateFunction(sum, UInt64)', unhex('018094EBDC0301781E41676772656761746546756E6374696F6E2873756D2C2055496E743634290000000000000000'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError CANNOT_READ_ALL_DATA }

-- The same for a state of zero size, where the row count alone drives the size of the column.
SELECT count() FROM format(Native, 'x AggregateFunction(nothing, UInt8)', unhex('018094EBDC0301782141676772656761746546756E6374696F6E286E6F7468696E672C2055496E7438290000000000000000'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError CANNOT_READ_ALL_DATA }

-- And for a state that serializes to no bytes at all: a block declaring a billion rows of it carries
-- nothing, and its count comes from a header the server did not write, so it must not be believed.
SELECT count() FROM format(Native, 'x AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)', unhex('018094EBDC0301783A41676772656761746546756E6374696F6E28636F756E74526573616D706C652831302C20352C2031292C2055496E7436342C2055496E74363429'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError CANNOT_READ_ALL_DATA }

-- A state of 2^40 bytes with 2^24 rows: the product of the two is exactly 2^64 and used to wrap
-- around to zero, so the states were created outside of the allocated block.
SELECT count() FROM format(Native, 'x AggregateFunction(countResampleIfResample(0, 1048576, 1, 0, 131072, 1), UInt64, UInt8, UInt64)', unhex('018080800801785E41676772656761746546756E6374696F6E28636F756E74526573616D706C654966526573616D706C6528302C20313034383537362C20312C20302C203133313037322C2031292C2055496E7436342C2055496E74382C2055496E743634290000000000000000'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Here the product lands just below the maximum of `size_t` (it is `2^64 - 16`) instead, and then
-- the padding and the rounding that the arena adds on top of it are what wraps around.
SELECT count() FROM format(Native, 'x AggregateFunction(countResampleIfResample(0, 19065, 1, 0, 121146, 1), UInt64, UInt8, UInt64)', unhex('01E3CB86DC0301785C41676772656761746546756E6374696F6E28636F756E74526573616D706C654966526573616D706C6528302C2031393036352C20312C20302C203132313134362C2031292C2055496E7436342C2055496E74382C2055496E743634290000000000000000'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The number of states can also come from the offsets of an array instead of the row count.
SELECT count() FROM format(Native, 'x Array(AggregateFunction(countResampleIfResample(0, 19065, 1, 0, 121146, 1), UInt64, UInt8, UInt64))', unhex('010101786341727261792841676772656761746546756E6374696F6E28636F756E74526573616D706C654966526573616D706C6528302C2031393036352C20312C20302C203132313134362C2031292C2055496E7436342C2055496E74382C2055496E7436342929E3A5813B000000000000000000000000'))
SETTINGS max_memory_usage = '1Gi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Reading a column of states back is not affected.
DROP TABLE IF EXISTS t_aggregate_states;
CREATE TABLE t_aggregate_states (k UInt8, s AggregateFunction(avg, UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_aggregate_states SELECT number % 251, initializeAggregation('avgState', number) FROM numbers(200000);
SELECT count(), avgMerge(s) FROM t_aggregate_states;
DROP TABLE t_aggregate_states;

-- A single state larger than one block of states is read on its own, so a column of them is read
-- one state at a time, whatever the number of rows a single read asks for.
DROP TABLE IF EXISTS t_large_aggregate_states;
CREATE TABLE t_large_aggregate_states (k UInt8, s AggregateFunction(countResample(0, 1048576, 1), UInt64, UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_large_aggregate_states SELECT number, countResampleState(0, 1048576, 1)(number, number) FROM numbers(3) GROUP BY number;
SELECT count(), arraySum(countResampleMerge(0, 1048576, 1)(s)) FROM t_large_aggregate_states;
DROP TABLE t_large_aggregate_states;

SELECT sumMerge(x) FROM format(Native, 'x AggregateFunction(sum, UInt64)', unhex('010101781E41676772656761746546756E6374696F6E2873756D2C2055496E743634292D00000000000000'));

-- A state can also serialize to no bytes at all, and then the end of the data says nothing about how
-- many states are left: `countResample` over an empty range holds no nested state and writes nothing.
-- Such a column used to read back as zero rows, which made a `Dynamic` column holding it fail on
-- every read and on every merge, leaving the table unreadable and unmergeable.
DROP TABLE IF EXISTS t_empty_state_dynamic;
CREATE TABLE t_empty_state_dynamic (k UInt64, s Dynamic) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_empty_state_dynamic SELECT number, countResampleState(10, 5, 1)(number, number) FROM numbers(100) GROUP BY number;
SELECT count(), uniqExact(dynamicType(s)) FROM t_empty_state_dynamic;

INSERT INTO t_empty_state_dynamic SELECT number, countResampleState(10, 5, 1)(number, number) FROM numbers(100, 100) GROUP BY number;
OPTIMIZE TABLE t_empty_state_dynamic FINAL;
SELECT count(), uniqExact(dynamicType(s)) FROM t_empty_state_dynamic;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_state_dynamic' AND active;
DROP TABLE t_empty_state_dynamic;

-- A compact part rejects the short read instead of returning fewer rows.
DROP TABLE IF EXISTS t_empty_state_compact;
CREATE TABLE t_empty_state_compact (k UInt64, s AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_empty_state_compact SELECT number, countResampleState(10, 5, 1)(number, number) FROM numbers(100) GROUP BY number;
SELECT count(), length(countResampleMerge(10, 5, 1)(s)) FROM t_empty_state_compact;
DROP TABLE t_empty_state_compact;

-- Combinators that write no bytes of their own around the nested state pass the property through.
DROP TABLE IF EXISTS t_empty_state_nested;
CREATE TABLE t_empty_state_nested (k UInt64, s Dynamic) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_empty_state_nested SELECT number, countResampleIfResampleState(10, 5, 1, 0, 5, 1)(number, 1, number) FROM numbers(100) GROUP BY number;
SELECT count(), uniqExact(dynamicType(s)) FROM t_empty_state_nested;
DROP TABLE t_empty_state_nested;

-- `-Tuple` holds the nested states back to back and frames nothing of its own, so a tuple of empty
-- ranges is empty too.
DROP TABLE IF EXISTS t_empty_state_tuple;
CREATE TABLE t_empty_state_tuple (k UInt64, s Dynamic) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_empty_state_tuple SELECT number, countResampleTupleState(10, 5, 1)((number, number), (number, number)) FROM numbers(100) GROUP BY number;
SELECT count(), uniqExact(dynamicType(s)) FROM t_empty_state_tuple;
DROP TABLE t_empty_state_tuple;

-- A nullable argument wraps the function in the `Null` combinator. When the result cannot be inside
-- `Nullable`, as an `Array` cannot, no flag byte is written and the state is again exactly the nested
-- one; a nested function that keeps its own flag byte stays out of this.
DROP TABLE IF EXISTS t_empty_state_if_null;
CREATE TABLE t_empty_state_if_null (k UInt64, s Dynamic) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_empty_state_if_null SELECT number, avgResampleIfState(10, 5, 1)(number, number, toNullable(1)) FROM numbers(100) GROUP BY number;
SELECT count(), uniqExact(dynamicType(s)) FROM t_empty_state_if_null;
DROP TABLE t_empty_state_if_null;

-- `Log` has a marks file and knows how many rows it holds. `TinyLog` has none, so a column of states
-- that occupy no bytes is unreadable there by construction, and rows must not be invented instead.
DROP TABLE IF EXISTS t_empty_state_log;
CREATE TABLE t_empty_state_log (k UInt64, s AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)) ENGINE = Log;
INSERT INTO t_empty_state_log SELECT number, countResampleState(10, 5, 1)(number, number) FROM numbers(100) GROUP BY number;
SELECT count(), length(countResampleMerge(10, 5, 1)(s)) FROM t_empty_state_log;
DROP TABLE t_empty_state_log;

-- `Log` decides whether it holds anything from its first data file, which such a column leaves empty.
-- `WHERE NOT ignore(s)` is what makes this read the column: a bare `count()` answers from the marks.
DROP TABLE IF EXISTS t_empty_state_log_first;
CREATE TABLE t_empty_state_log_first (s AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)) ENGINE = Log;
INSERT INTO t_empty_state_log_first SELECT countResampleState(10, 5, 1)(number, number) FROM numbers(100) GROUP BY number;
SELECT count() FROM t_empty_state_log_first WHERE NOT ignore(s);
DROP TABLE t_empty_state_log_first;

-- An empty `Log` table still reads as no rows through both paths.
DROP TABLE IF EXISTS t_empty_state_log_none;
CREATE TABLE t_empty_state_log_none (s AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)) ENGINE = Log;
SELECT count() FROM t_empty_state_log_none;
SELECT count() FROM t_empty_state_log_none WHERE NOT ignore(s) SETTINGS max_threads = 4;
DROP TABLE t_empty_state_log_none;

DROP TABLE IF EXISTS t_empty_state_tiny_log;
CREATE TABLE t_empty_state_tiny_log (k UInt64, s AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)) ENGINE = TinyLog;
INSERT INTO t_empty_state_tiny_log SELECT number, countResampleState(10, 5, 1)(number, number) FROM numbers(100) GROUP BY number;
SELECT count() FROM (SELECT s FROM t_empty_state_tiny_log);
DROP TABLE t_empty_state_tiny_log;

-- A state of no size whose serialized form is not empty keeps the end of the data as its stop
-- condition: `nothing` writes one byte, and `nothingResample` writes one for each element of its range.
DROP TABLE IF EXISTS t_nothing_states;
CREATE TABLE t_nothing_states (k UInt64, s AggregateFunction(nothing, UInt8), r AggregateFunction(nothingResample(0, 1024, 1), UInt8, UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_nothing_states SELECT number, initializeAggregation('nothingState', 1::UInt8), nothingResampleState(0, 1024, 1)(1::UInt8, number) FROM numbers(100) GROUP BY number;
SELECT count() FROM t_nothing_states WHERE NOT ignore(s, r);
DROP TABLE t_nothing_states;
