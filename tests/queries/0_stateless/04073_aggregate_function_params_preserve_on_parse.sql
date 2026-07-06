SELECT toTypeName(CAST(groupArrayMovingSumState(2)(toUInt64(number)), 'AggregateFunction(groupArrayMovingSum(2), UInt64)')) FROM numbers(1);
