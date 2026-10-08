-- Regression test: `mapPopulateSeries` triggered signed integer overflow at runtime
-- under UBSan when the difference `max_key - min_key` didn't fit into the key type
-- (e.g. `Int8` with `min = -94`, `max = 34`, where `34 - (-94) = 128` overflows int8).

SELECT length((mapPopulateSeries([toInt8(-94), toInt8(34)], [toInt8(1), toInt8(2)])).1);
SELECT length((mapPopulateSeries([toInt8(-128), toInt8(127)], [toInt8(1), toInt8(2)])).1);
SELECT length((mapPopulateSeries([toInt16(-32000), toInt16(32000)], [toInt8(1), toInt8(2)])).1);
