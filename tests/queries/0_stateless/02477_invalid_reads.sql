-- MIN, MAX AND FAMILY should check for errors in its input
SELECT finalizeAggregation(CAST(unhex('0F00000030'), 'AggregateFunction(min, String)')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('FFFF000030'), 'AggregateFunction(min, String)')); -- { serverError CANNOT_READ_ALL_DATA }

-- UBSAN
SELECT 'ubsan', hex(finalizeAggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233010000000000000000'),
                                             'AggregateFunction(argMax, String, UInt64)')));

-- aggThrow should check for errors in its input
SELECT finalizeAggregation(CAST('', 'AggregateFunction(aggThrow(0.), UInt8)')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- categoricalInformationValue should check for errors in its input
SELECT finalizeAggregation(CAST(unhex('01000000000000000100000000000000'),
                                'AggregateFunction(categoricalInformationValue, UInt8, UInt8)')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('0101000000000000000100000000000000020000000000000001000000000000'),
    'AggregateFunction(categoricalInformationValue, Nullable(UInt8), UInt8)')); -- { serverError CANNOT_READ_ALL_DATA }

-- groupArray should check for errors in its input
SELECT finalizeAggregation(CAST(unhex('5FF3001310132'), 'AggregateFunction(groupArray, String)'));  -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('FF000000000000000001000000000000000200000000000000'), 'AggregateFunction(groupArray, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }

-- Same for groupArrayMovingXXXX
SELECT finalizeAggregation(CAST(unhex('0FF00000000000000001000000000000000300000000000000'), 'AggregateFunction(groupArrayMovingSum, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('0FF00000000000000001000000000000000300000000000000'), 'AggregateFunction(groupArrayMovingAvg, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }

-- Histogram
SELECT finalizeAggregation(CAST(unhex('00000000000024C000000000000018C00500000000000024C0000000000000F03F00000000000022C0000000000000F03F00000000000020C0000000000000'),
    'AggregateFunction(histogram(5), Int64)')); -- { serverError CANNOT_READ_ALL_DATA }

-- StatisticalSample
SELECT finalizeAggregation(CAST(unhex('0F01000000000000244000000000000026400000000000002840000000000000244000000000000026400000000000002840000000000000F03F'),
                                'AggregateFunction(mannWhitneyUTest, Float64, UInt8)')); -- { serverError CANNOT_READ_ALL_DATA }

-- maxIntersections
SELECT finalizeAggregation(CAST(unhex('0F010000000000000001000000000000000300000000000000FFFFFFFFFFFFFFFF03340B9B047F000001000000000000000500000065000000FFFFFFFFFFFFFFFF'),
                                'AggregateFunction(maxIntersections, UInt8, UInt8)')); -- { serverError CANNOT_READ_ALL_DATA }

-- sequenceNextNode (This was fine because it would fail in the next readBinary call, but better to add a test)
SELECT finalizeAggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'AggregateFunction(sequenceNextNode(''forward'', ''head''), DateTime, Nullable(String), UInt8, Nullable(UInt8))'))
    SETTINGS allow_experimental_funnel_functions=1; -- { serverError CANNOT_READ_ALL_DATA }

-- Fuzzer (ALL)
SELECT finalizeAggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'AggregateFunction(sequenceNextNode(\'forward\', \'head\'), DateTime, Nullable(String), UInt8, Nullable(UInt8))'))
    SETTINGS allow_experimental_funnel_functions = 1; -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Fuzzer 2 (UBSAN)
SELECT finalizeAggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'AggregateFunction(sequenceNextNode(\'forward\', \'head\'), DateTime, Nullable(String), UInt8, Nullable(UInt8))'))
    SETTINGS allow_experimental_funnel_functions = 1; -- { serverError CANNOT_READ_ALL_DATA }

-- uniqUpTo
SELECT finalizeAggregation(CAST(unhex('04128345AA2BC97190'),
                                'AggregateFunction(uniqUpTo(10), String)')); -- { serverError CANNOT_READ_ALL_DATA }

-- quantiles
SELECT finalizeAggregation(CAST(unhex('0F0000000000000000'),
                                'AggregateFunction(quantileExact, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('0F000000000000803F'),
                                'AggregateFunction(quantileTDigest, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }
