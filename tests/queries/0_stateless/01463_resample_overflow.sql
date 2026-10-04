select groupArrayResample(-9223372036854775808, 9223372036854775807, 9223372036854775807)(number, toInt64(number)) FROM numbers(7); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The number of subintervals of one Resample layer is capped; the cap itself is still accepted.
select arrayReduce('countResample(0, 1048577, 1)', [0]); -- { serverError ARGUMENT_OUT_OF_BOUND }
select length(arrayReduce('countResample(0, 1048576, 1)', [0]));
