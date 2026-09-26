-- Correctness of groupBitOr/groupBitAnd/groupBitXor batch paths for all integer
-- types: plain, -If, Nullable and Nullable + -If, verified against arrayReduce,
-- which aggregates the same values through the scalar per-row add instead of
-- addBatchSinglePlace; plus identity values for empty selections.

SELECT
    'UInt8',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toUInt8(bitAnd(cityHash64(number, 1), 255)) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt8 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt8'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt8')), groupBitXorIf(v, c0) = CAST(0, 'UInt8') FROM (SELECT toUInt8(bitAnd(cityHash64(number, 1), 255)) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt8 empty', groupBitOr(v) = CAST(0, 'UInt8'), groupBitAnd(v) = bitNot(CAST(0, 'UInt8')), groupBitXor(v) = CAST(0, 'UInt8') FROM (SELECT toUInt8(bitAnd(cityHash64(number, 1), 255)) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt8 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toUInt8(bitAnd(cityHash64(number, 1), 255))) AS vn FROM numbers(100));

SELECT
    'UInt16',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toUInt16(bitAnd(cityHash64(number, 1), 65535)) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt16 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt16'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt16')), groupBitXorIf(v, c0) = CAST(0, 'UInt16') FROM (SELECT toUInt16(bitAnd(cityHash64(number, 1), 65535)) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt16 empty', groupBitOr(v) = CAST(0, 'UInt16'), groupBitAnd(v) = bitNot(CAST(0, 'UInt16')), groupBitXor(v) = CAST(0, 'UInt16') FROM (SELECT toUInt16(bitAnd(cityHash64(number, 1), 65535)) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt16 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toUInt16(bitAnd(cityHash64(number, 1), 65535))) AS vn FROM numbers(100));

SELECT
    'UInt32',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toUInt32(bitAnd(cityHash64(number, 1), 4294967295)) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt32 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt32'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt32')), groupBitXorIf(v, c0) = CAST(0, 'UInt32') FROM (SELECT toUInt32(bitAnd(cityHash64(number, 1), 4294967295)) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt32 empty', groupBitOr(v) = CAST(0, 'UInt32'), groupBitAnd(v) = bitNot(CAST(0, 'UInt32')), groupBitXor(v) = CAST(0, 'UInt32') FROM (SELECT toUInt32(bitAnd(cityHash64(number, 1), 4294967295)) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt32 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toUInt32(bitAnd(cityHash64(number, 1), 4294967295))) AS vn FROM numbers(100));

SELECT
    'UInt64',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT cityHash64(number, 1) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt64 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt64'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt64')), groupBitXorIf(v, c0) = CAST(0, 'UInt64') FROM (SELECT cityHash64(number, 1) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt64 empty', groupBitOr(v) = CAST(0, 'UInt64'), groupBitAnd(v) = bitNot(CAST(0, 'UInt64')), groupBitXor(v) = CAST(0, 'UInt64') FROM (SELECT cityHash64(number, 1) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt64 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, cityHash64(number, 1)) AS vn FROM numbers(100));

SELECT
    'UInt128',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128')) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt128 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt128'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt128')), groupBitXorIf(v, c0) = CAST(0, 'UInt128') FROM (SELECT bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128')) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt128 empty', groupBitOr(v) = CAST(0, 'UInt128'), groupBitAnd(v) = bitNot(CAST(0, 'UInt128')), groupBitXor(v) = CAST(0, 'UInt128') FROM (SELECT bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128')) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt128 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128'))) AS vn FROM numbers(100));

SELECT
    'UInt256',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256'))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'UInt256 identity', groupBitOrIf(v, c0) = CAST(0, 'UInt256'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'UInt256')), groupBitXorIf(v, c0) = CAST(0, 'UInt256') FROM (SELECT bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256'))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'UInt256 empty', groupBitOr(v) = CAST(0, 'UInt256'), groupBitAnd(v) = bitNot(CAST(0, 'UInt256')), groupBitXor(v) = CAST(0, 'UInt256') FROM (SELECT bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256'))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'UInt256 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256')))) AS vn FROM numbers(100));

SELECT
    'Int8',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt8(toUInt8(bitAnd(cityHash64(number, 1), 255))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int8 identity', groupBitOrIf(v, c0) = CAST(0, 'Int8'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int8')), groupBitXorIf(v, c0) = CAST(0, 'Int8') FROM (SELECT toInt8(toUInt8(bitAnd(cityHash64(number, 1), 255))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int8 empty', groupBitOr(v) = CAST(0, 'Int8'), groupBitAnd(v) = bitNot(CAST(0, 'Int8')), groupBitXor(v) = CAST(0, 'Int8') FROM (SELECT toInt8(toUInt8(bitAnd(cityHash64(number, 1), 255))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int8 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt8(toUInt8(bitAnd(cityHash64(number, 1), 255)))) AS vn FROM numbers(100));

SELECT
    'Int16',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt16(toUInt16(bitAnd(cityHash64(number, 1), 65535))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int16 identity', groupBitOrIf(v, c0) = CAST(0, 'Int16'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int16')), groupBitXorIf(v, c0) = CAST(0, 'Int16') FROM (SELECT toInt16(toUInt16(bitAnd(cityHash64(number, 1), 65535))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int16 empty', groupBitOr(v) = CAST(0, 'Int16'), groupBitAnd(v) = bitNot(CAST(0, 'Int16')), groupBitXor(v) = CAST(0, 'Int16') FROM (SELECT toInt16(toUInt16(bitAnd(cityHash64(number, 1), 65535))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int16 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt16(toUInt16(bitAnd(cityHash64(number, 1), 65535)))) AS vn FROM numbers(100));

SELECT
    'Int32',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt32(toUInt32(bitAnd(cityHash64(number, 1), 4294967295))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int32 identity', groupBitOrIf(v, c0) = CAST(0, 'Int32'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int32')), groupBitXorIf(v, c0) = CAST(0, 'Int32') FROM (SELECT toInt32(toUInt32(bitAnd(cityHash64(number, 1), 4294967295))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int32 empty', groupBitOr(v) = CAST(0, 'Int32'), groupBitAnd(v) = bitNot(CAST(0, 'Int32')), groupBitXor(v) = CAST(0, 'Int32') FROM (SELECT toInt32(toUInt32(bitAnd(cityHash64(number, 1), 4294967295))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int32 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt32(toUInt32(bitAnd(cityHash64(number, 1), 4294967295)))) AS vn FROM numbers(100));

SELECT
    'Int64',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt64(cityHash64(number, 1)) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int64 identity', groupBitOrIf(v, c0) = CAST(0, 'Int64'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int64')), groupBitXorIf(v, c0) = CAST(0, 'Int64') FROM (SELECT toInt64(cityHash64(number, 1)) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int64 empty', groupBitOr(v) = CAST(0, 'Int64'), groupBitAnd(v) = bitNot(CAST(0, 'Int64')), groupBitXor(v) = CAST(0, 'Int64') FROM (SELECT toInt64(cityHash64(number, 1)) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int64 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt64(cityHash64(number, 1))) AS vn FROM numbers(100));

SELECT
    'Int128',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt128(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128'))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int128 identity', groupBitOrIf(v, c0) = CAST(0, 'Int128'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int128')), groupBitXorIf(v, c0) = CAST(0, 'Int128') FROM (SELECT toInt128(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128'))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int128 empty', groupBitOr(v) = CAST(0, 'Int128'), groupBitAnd(v) = bitNot(CAST(0, 'Int128')), groupBitXor(v) = CAST(0, 'Int128') FROM (SELECT toInt128(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128'))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int128 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt128(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt128'), 64), CAST(cityHash64(number, 2), 'UInt128')))) AS vn FROM numbers(100));

SELECT
    'Int256',
    gbOr = arrayReduce('groupBitOr', arr),
    gbAnd = arrayReduce('groupBitAnd', arr),
    gbXor = arrayReduce('groupBitXor', arr),
    gbOrIf = arrayReduce('groupBitOr', arrIf),
    gbAndIf = arrayReduce('groupBitAnd', arrIf),
    gbXorIf = arrayReduce('groupBitXor', arrIf),
    gbOrNull = arrayReduce('groupBitOr', arrNull),
    gbAndNull = arrayReduce('groupBitAnd', arrNull),
    gbXorNull = arrayReduce('groupBitXor', arrNull),
    gbOrNullIf = arrayReduce('groupBitOr', arrNullIf),
    gbAndNullIf = arrayReduce('groupBitAnd', arrNullIf),
    gbXorNullIf = arrayReduce('groupBitXor', arrNullIf)
FROM
(
    SELECT
        groupBitOr(v) AS gbOr,
        groupBitAnd(v) AS gbAnd,
        groupBitXor(v) AS gbXor,
        groupBitOrIf(v, c) AS gbOrIf,
        groupBitAndIf(v, c) AS gbAndIf,
        groupBitXorIf(v, c) AS gbXorIf,
        groupBitOr(vn) AS gbOrNull,
        groupBitAnd(vn) AS gbAndNull,
        groupBitXor(vn) AS gbXorNull,
        groupBitOrIf(vn, c) AS gbOrNullIf,
        groupBitAndIf(vn, c) AS gbAndNullIf,
        groupBitXorIf(vn, c) AS gbXorNullIf,
        groupArray(v) AS arr,
        groupArrayIf(v, c) AS arrIf,
        groupArray(vn) AS arrNull,
        groupArrayIf(vn, c) AS arrNullIf
    FROM (SELECT toInt256(bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256')))) AS v, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))
);

SELECT 'Int256 identity', groupBitOrIf(v, c0) = CAST(0, 'Int256'), groupBitAndIf(v, c0) = bitNot(CAST(0, 'Int256')), groupBitXorIf(v, c0) = CAST(0, 'Int256') FROM (SELECT toInt256(bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256')))) AS v, number > 100 AS c0 FROM numbers(100));
SELECT 'Int256 empty', groupBitOr(v) = CAST(0, 'Int256'), groupBitAnd(v) = bitNot(CAST(0, 'Int256')), groupBitXor(v) = CAST(0, 'Int256') FROM (SELECT toInt256(bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256')))) AS v FROM numbers(100) WHERE number > 100);
SELECT 'Int256 allnull', isNull(groupBitOr(vn)), isNull(groupBitAnd(vn)), isNull(groupBitXor(vn)) FROM (SELECT if(number != 999, NULL, toInt256(bitOr(bitOr(bitShiftLeft(CAST(cityHash64(number, 1), 'UInt256'), 192), bitShiftLeft(CAST(cityHash64(number, 2), 'UInt256'), 128)), bitOr(bitShiftLeft(CAST(cityHash64(number, 3), 'UInt256'), 64), CAST(cityHash64(number, 4), 'UInt256'))))) AS vn FROM numbers(100));

-- Constructed non-saturating masking check. The random cross-checks above OR to all-ones and
-- AND to all-zeros over 70k rows, so groupBitOrIf/groupBitAndIf reach the right answer regardless
-- of which rows their keep_mask selects. Here bit 0 is set only in kept rows and the top bit only
-- in dropped rows, so groupBitOrIf(v, c) != groupBitOr(v) and groupBitAndIf(v, c) != groupBitAnd(v):
-- each -If kernel proves its own masking formula instead of leaning on groupBitXorIf as a proxy.
-- Covers the scalar path (UInt8) and the wide-integer memset path (UInt256), plain and Nullable.
SELECT 'UInt8 masked',
    groupBitOrIf(v, c)  = arrayReduce('groupBitOr',  groupArrayIf(v, c)),
    groupBitAndIf(v, c) = arrayReduce('groupBitAnd', groupArrayIf(v, c)),
    groupBitXorIf(v, c) = arrayReduce('groupBitXor', groupArrayIf(v, c)),
    groupBitOrIf(vn, c)  = arrayReduce('groupBitOr',  groupArrayIf(vn, c)),
    groupBitAndIf(vn, c) = arrayReduce('groupBitAnd', groupArrayIf(vn, c)),
    groupBitXorIf(vn, c) = arrayReduce('groupBitXor', groupArrayIf(vn, c)),
    groupBitOrIf(v, c)  != groupBitOr(v),
    groupBitAndIf(v, c) != groupBitAnd(v)
FROM (SELECT toUInt8(bitOr(if(bitAnd(number, 1) = 0, 1, 128), bitShiftLeft(bitAnd(number, 15), 1))) AS v, bitAnd(number, 1) = 0 AS c, if(bitAnd(number, 3) = 1, NULL, toUInt8(bitOr(if(bitAnd(number, 1) = 0, 1, 128), bitShiftLeft(bitAnd(number, 15), 1)))) AS vn FROM numbers(14));

SELECT 'UInt256 masked',
    groupBitOrIf(v, c)  = arrayReduce('groupBitOr',  groupArrayIf(v, c)),
    groupBitAndIf(v, c) = arrayReduce('groupBitAnd', groupArrayIf(v, c)),
    groupBitXorIf(v, c) = arrayReduce('groupBitXor', groupArrayIf(v, c)),
    groupBitOrIf(vn, c)  = arrayReduce('groupBitOr',  groupArrayIf(vn, c)),
    groupBitAndIf(vn, c) = arrayReduce('groupBitAnd', groupArrayIf(vn, c)),
    groupBitXorIf(vn, c) = arrayReduce('groupBitXor', groupArrayIf(vn, c)),
    groupBitOrIf(v, c)  != groupBitOr(v),
    groupBitAndIf(v, c) != groupBitAnd(v)
FROM (SELECT bitOr(if(bitAnd(number, 1) = 0, CAST(1, 'UInt256'), bitShiftLeft(CAST(1, 'UInt256'), 255)), CAST(bitShiftLeft(bitAnd(number, 15), 1), 'UInt256')) AS v, bitAnd(number, 1) = 0 AS c, if(bitAnd(number, 3) = 1, NULL, bitOr(if(bitAnd(number, 1) = 0, CAST(1, 'UInt256'), bitShiftLeft(CAST(1, 'UInt256'), 255)), CAST(bitShiftLeft(bitAnd(number, 15), 1), 'UInt256'))) AS vn FROM numbers(14));
