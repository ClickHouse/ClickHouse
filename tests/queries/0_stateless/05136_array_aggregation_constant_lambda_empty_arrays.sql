SELECT
    id,
    arr,
    arrayMin(x -> 7, arr),
    arrayMax(x -> 7, arr),
    arraySum(x -> 7, arr),
    arrayAvg(x -> 7, arr),
    arrayProduct(x -> 7, arr)
FROM values('id UInt8, arr Array(Int32)', (1, []), (2, []), (3, [1, 2]), (4, [3]), (5, []), (6, []))
ORDER BY id;

SELECT
    id,
    arr,
    arrayMin(x -> toFloat64('nan'), arr),
    arrayMax(x -> toFloat64('nan'), arr),
    arraySum(x -> toFloat64('nan'), arr),
    arrayAvg(x -> toFloat64('nan'), arr),
    arrayProduct(x -> toFloat64('nan'), arr)
FROM values('id UInt8, arr Array(Float64)', (1, []), (2, []), (3, [1, 2]), (4, [3]), (5, []), (6, []))
ORDER BY id;

SELECT
    id,
    arr,
    arrayMin(x -> toDecimal32(7, 2), arr),
    arrayMax(x -> toDecimal32(7, 2), arr),
    arraySum(x -> toDecimal32(7, 2), arr),
    arrayAvg(x -> toDecimal32(7, 2), arr),
    arrayProduct(x -> toDecimal32(7, 2), arr)
FROM values('id UInt8, arr Array(Decimal32(2))', (1, []), (2, []), (3, [1, 2]), (4, [3]), (5, []), (6, []))
ORDER BY id;

SELECT
    id,
    arr,
    length(arrayMin(x -> 'foo', arr)),
    length(arrayMax(x -> 'foo', arr))
FROM values('id UInt8, arr Array(Int32)', (1, []), (2, []), (3, [1, 2]), (4, [3]), (5, []), (6, []))
ORDER BY id;
