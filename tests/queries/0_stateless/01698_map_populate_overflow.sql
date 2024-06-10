SELECT mapPopulateSeries([0xFFFFFFFFFFFFFFFF], [0], 0xFFFFFFFFFFFFFFFF);
SELECT mapPopulateSeries([toUInt64(1)], [1], 0xFFFFFFFFFFFFFFFF); -- { serverError 128 }
