-- arrayMin/arrayMax must return the first NaN of an all-NaN array and the first zero when the extreme is zero,
-- bitwise-identical to the generic compareAt path.

SELECT hex(reinterpretAsUInt64(arrayMin(a))), hex(reinterpretAsUInt64(arrayMax(a))) FROM (SELECT materialize([reinterpret(toUInt64(0xFFF800000000000A), 'Float64'), reinterpret(toUInt64(0x7FF8000000000001), 'Float64')]) AS a);
SELECT hex(reinterpretAsUInt64(arrayMin(a))), hex(reinterpretAsUInt64(arrayMax(a))) FROM (SELECT materialize([toFloat64(0), reinterpret(toUInt64(0x8000000000000000), 'Float64')]) AS a);
SELECT hex(reinterpretAsUInt64(arrayMin(a))), hex(reinterpretAsUInt64(arrayMax(a))) FROM (SELECT materialize([reinterpret(toUInt64(0x8000000000000000), 'Float64'), toFloat64(0)]) AS a);
SELECT hex(reinterpretAsUInt64(arrayMin(a))), hex(reinterpretAsUInt64(arrayMax(a))) FROM (SELECT materialize([toFloat64(1), reinterpret(toUInt64(0x8000000000000000), 'Float64'), toFloat64(0)]) AS a);

SELECT hex(reinterpretAsUInt32(arrayMin(a))), hex(reinterpretAsUInt32(arrayMax(a))) FROM (SELECT materialize([reinterpret(toUInt32(0xFFC0000A), 'Float32'), reinterpret(toUInt32(0x7FC00001), 'Float32')]) AS a);
SELECT hex(reinterpretAsUInt32(arrayMin(a))), hex(reinterpretAsUInt32(arrayMax(a))) FROM (SELECT materialize([toFloat32(0), reinterpret(toUInt32(0x80000000), 'Float32')]) AS a);
SELECT hex(reinterpretAsUInt32(arrayMin(a))), hex(reinterpretAsUInt32(arrayMax(a))) FROM (SELECT materialize([reinterpret(toUInt32(0x80000000), 'Float32'), toFloat32(0)]) AS a);
SELECT hex(reinterpretAsUInt32(arrayMin(a))), hex(reinterpretAsUInt32(arrayMax(a))) FROM (SELECT materialize([toFloat32(-1), toFloat32(0), reinterpret(toUInt32(0x80000000), 'Float32')]) AS a);
