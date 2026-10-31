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
