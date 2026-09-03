SELECT '\x01\x00'::AggregateFunction(groupBitmap, UInt32); -- { serverError INCORRECT_DATA }
SELECT '\x01\x01\x01'::AggregateFunction(groupBitmap, UInt64); -- { serverError STD_EXCEPTION }
SELECT '\x02\x00\x0d'::AggregateFunction(topK, UInt256); -- { serverError CANNOT_READ_ALL_DATA }
SELECT unhex('bebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebe0c0c3131313131313131313131313173290aee00b300')::AggregateFunction(minDistinct, Int8); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT unhex('01000b0b0b0d0d0d0d7175616e74696c6554696d696e672c20496e743332000300')::AggregateFunction(quantileTiming, Int32); -- { serverError INCORRECT_DATA }
SELECT unhex('010001')::AggregateFunction(quantileTiming, Int32); -- { serverError INCORRECT_DATA }
SELECT unhex('0a00797979797979797979790a0a6e')::AggregateFunction(minForEach, Ring); -- { serverError TOO_LARGE_ARRAY_SIZE }
