SELECT [2, 1, 3] AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -x, arr);
SELECT materialize([2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -x, arr);

SELECT arrayMap(x -> toString(x), [2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> reverse(x), arr);
SELECT arrayMap(x -> toString(x), materialize([2, 1, 3])) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> reverse(x), arr);

SELECT arrayMap(x -> range(x), [2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -length(x), arr);
SELECT arrayMap(x -> range(x), materialize([2, 1, 3])) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -length(x), arr);

SELECT splitByChar('0', toString(intHash64(number))) AS arr, arraySort(arr) AS sorted, arraySort(x -> toUInt64OrZero(x), arr) AS sorted_nums FROM system.numbers LIMIT 10;

SELECT arrayReverseSort(number % 2 ? emptyArrayUInt64() : range(number)) FROM system.numbers LIMIT 10;

SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
