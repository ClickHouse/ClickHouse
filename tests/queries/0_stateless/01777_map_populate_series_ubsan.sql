-- Should correctly throw exception about overflow:
SELECT mapPopulateSeries([-9223372036854775808, toUInt32(2)], [toUInt32(1023), -1]); -- { serverError TOO_LARGE_ARRAY_SIZE }
