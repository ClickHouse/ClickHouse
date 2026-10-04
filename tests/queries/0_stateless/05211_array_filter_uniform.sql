SELECT arrayFilter(
    (x, keep) -> keep,
    arrayMap(x -> x + 1, range(number % 4)),
    arrayMap(x -> toUInt8(x < number % 4), range(number % 4)))
FROM numbers(4);
SELECT arrayFilter(
    (x, keep) -> keep,
    arrayMap(x -> x + 1, range(number % 4)),
    arrayMap(x -> toUInt8(x >= number % 4), range(number % 4)))
FROM numbers(4);
SELECT arrayFilter(
    (x, keep) -> keep,
    arrayMap(x -> x, range(number % 4)),
    arrayMap(x -> toUInt8(number % 2), range(number % 4)))
FROM numbers(4);

SELECT arrayFilter((x, keep) -> keep, [1, 2, 3], materialize([1, 1, 1]));
SELECT arrayFilter((x, keep) -> keep, [1, 2, 3], materialize([0, 0, 0]));
SELECT arrayFilter((x, keep) -> keep, [1, 2, 3], materialize([1, 0, 1]));
SELECT arrayFilter((x, keep) -> keep, [1, 2, 3], materialize([2, 2, 2]));

SELECT arrayFilter((x, keep) -> keep, ['a', 'bb', 'ccc'], materialize([1, 1, 1]));
SELECT arrayFilter((x, keep) -> keep, ['a', 'bb', 'ccc'], materialize([0, 0, 0]));
SELECT arrayFilter((x, keep) -> keep, ['a', 'bb', 'ccc'], materialize([1, 0, 1]));

SELECT arraySum(x -> tupleElement(x, 1), arrayFilter((x, keep) -> keep, [(1, 'a'), (2, 'b')], materialize([1, 1])));
SELECT arraySum(x -> tupleElement(x, 1), arrayFilter((x, keep) -> keep, [(1, 'a'), (2, 'b')], materialize([0, 0])));

SELECT arrayFilter((x, keep) -> keep, CAST([1, NULL, 2], 'Array(Nullable(Int8))'), materialize([1, 1, 1]));
SELECT arrayFilter((x, keep) -> keep, CAST([1, NULL, 2], 'Array(Nullable(Int8))'), materialize([0, 0, 0]));
SELECT arrayFilter((x, keep) -> keep, [1, 2, 3], materialize([NULL, 1, 1]));

SELECT arrayFilter((x, keep) -> keep, emptyArrayUInt64(), materialize(emptyArrayUInt8()));
