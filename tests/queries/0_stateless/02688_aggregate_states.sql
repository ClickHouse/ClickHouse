SELECT '\x01\x00'::AggregateFunction(groupBitmap, UInt32); -- { serverError INCORRECT_DATA }
SELECT '\x01\x01\x01'::AggregateFunction(groupBitmap, UInt64); -- { serverError STD_EXCEPTION }
SELECT '\x02\x00\x0d'::AggregateFunction(topK, UInt256); -- { serverError CANNOT_READ_ALL_DATA }
SELECT unhex('bebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebe0c0c3131313131313131313131313173290aee00b300')::AggregateFunction(minDistinct, Int8); -- { serverError TOO_LARGE_ARRAY_SIZE }
