-- https://github.com/ClickHouse/ClickHouse/issues/43247
-- The size prefix of this state is the first four characters, 'Aggr', which declare almost
-- 2 GiB while the state carries ~130 bytes. That size is allocated before the data is found
-- to be short, so under memory pressure the allocation fails first and the query returns
-- MEMORY_LIMIT_EXCEEDED instead of CANNOT_READ_ALL_DATA. Both outcomes are correct - the
-- point of the test is that a corrupted state is rejected - so accept either code.
SELECT finalizeAggregation(CAST('AggregateFunction(categoricalInformationValue, Nullable(UInt8), UInt8)AggregateFunction(categoricalInformationValue, Nullable(UInt8), UInt8)',
                           'AggregateFunction(min, String)')); -- { serverError CANNOT_READ_ALL_DATA, MEMORY_LIMIT_EXCEEDED }

-- Value from hex(minState('0123456789012345678901234567890123456789012345678901234567890123')). Size 63 + 1 (64)
SELECT finalizeAggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233'),
                           'AggregateFunction(min, String)'));
