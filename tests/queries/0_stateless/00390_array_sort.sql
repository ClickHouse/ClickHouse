SELECT [2, 1, 3] AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -x, arr);
SELECT materialize([2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -x, arr);

SELECT arrayMap(x -> toString(x), [2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> reverse(x), arr);
SELECT arrayMap(x -> toString(x), materialize([2, 1, 3])) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> reverse(x), arr);

SELECT arrayMap(x -> range(x), [2, 1, 3]) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -length(x), arr);
SELECT arrayMap(x -> range(x), materialize([2, 1, 3])) AS arr, arraySort(arr), arrayReverseSort(arr), arraySort(x -> -length(x), arr);

SELECT splitByChar('0', toString(intHash64(number))) AS arr, arraySort(arr) AS sorted, arraySort(x -> toUInt64OrZero(x), arr) AS sorted_nums FROM system.numbers LIMIT 10;

SELECT arrayReverseSort(number % 2 ? emptyArrayUInt64() : range(number)) FROM system.numbers LIMIT 10;

SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);

-- Using arrayResize to trim the unsorted bit of the array that is normally left in unspecified order
SELECT [9,4,8,10,5,2,3,7,1,6] AS arr, 4 AS lim, arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> -x, lim, arr), lim);
SELECT materialize([9,4,8,10,5,2,3,7,1,6]) AS arr, 4 AS lim, arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> -x, lim, arr), lim);

SELECT arrayMap(x -> toString(x), [9,4,8,10,5,2,3,7,1,6]) AS arr, 4 AS lim, arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> reverse(x), lim, arr), lim);
SELECT arrayMap(x -> toString(x), materialize([9,4,8,10,5,2,3,7,1,6])) AS arr, 4 AS lim, arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> reverse(x), lim, arr), lim);

SELECT arrayMap(x -> range(x), [4,1,2,3]) AS arr, 2 AS lim, arrayResize(arrayPartialSort(lim, arr), lim), arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> -length(x), lim, arr), lim);
SELECT arrayMap(x -> range(x), materialize([4,1,2,3])) AS arr, 2 AS lim, arrayResize(arrayPartialSort(lim, arr), lim), arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> -length(x), lim, arr), lim);

SELECT splitByChar('0', toString(intHash64(number))) AS arr, 3 AS lim, arrayResize(arrayPartialSort(lim, arr), lim) AS sorted, arrayResize(arrayPartialSort(x -> toUInt64OrZero(x), lim, arr), lim) AS sorted_nums FROM system.numbers LIMIT 10;

SELECT res FROM (SELECT arrayPartialReverseSort(2, number % 2 ? emptyArrayUInt64() : range(number)) AS arr, arrayResize(arr, if(empty(arr), 0, 2)) AS res FROM system.numbers LIMIT 10);

SELECT arrayResize(arrayPartialSort((x, y) -> y, 3, ['directly','ignore','wrongly','relocate','upright'], [4,2,1,3,5]), 3);

SELECT 2 as nelems, arrayResize(arrayPartialSort(nelems, [NULL,9,4,8,10,5,2,3,7,1,6]), nelems);
SELECT 2 as nelems, arrayResize(arrayPartialSort(nelems, [NULL,'9','4','8','10','5','2','3','7','1','6']), nelems);
SELECT 2 as nelems, arrayResize(arrayPartialReverseSort(nelems, [NULL,9,4,8,10,5,2,3,7,1,6]), nelems);
SELECT 2 as nelems, arrayResize(arrayPartialReverseSort(nelems, [NULL,'9','4','8','10','5','2','3','7','1','6']), nelems);


SELECT 4 as nelems, arrayResize(arrayPartialSort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 4 as nelems, arrayResize(arrayPartialSort((x) -> -x, nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 10 as nelems, arrayResize(arrayPartialSort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 10 as nelems, arrayResize(arrayPartialSort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);

SELECT 3 as nelems, [[1,2],[-10,-20],[10,20],[0,0],[-1.5,1]] as arr, arrayResize(arrayPartialSort(nelems, arr), nelems), arrayResize(arrayPartialReverseSort(nelems, arr), nelems), arrayResize(arrayPartialSort((x) -> arraySum(x), nelems, arr), nelems);

SELECT 0 as nelems, [NULL,9,4,8,10,5,2,3,7,1,6] AS arr, arrayPartialSort(nelems, arr), arrayPartialReverseSort(nelems, arr), arrayPartialSort((x) -> -x, nelems, arr);
SELECT 10 as nelems, [NULL,9,4,8,10,5,2,3,7,1,6] AS arr, arrayPartialSort(nelems, arr), arrayPartialReverseSort(nelems, arr), arrayPartialSort((x) -> -x, nelems, arr);


SELECT arrayPartialSort([1,2,3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayPartialSort(2, [1,2,3], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialSort(2, [1,2,3], 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialSort(arraySort([1,2,3]), [1,2,3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayMap(x -> range(x), [4, 1, 2, 3]) AS arr, 100 AS lim, arrayResize(arrayPartialSort(arrayPartialSort(lim, arr), arr), lim), arrayResize(arrayPartialReverseSort(lim, arr), lim), arrayResize(arrayPartialSort(x -> (-length(x)), lim, arr), lim); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialReverseSort(arraySort((x, y) -> y, [NULL, NULL], [NULL, NULL]), arr), arrayMap(x -> toString(x), [257, -9223372036854775807, 2, -2147483648, 2147483648, NULL, 65536, -2147483648, 2, 65535]) AS arr, NULL, 100 AS lim, 65536, arrayResize(arrayPartialSort(x -> reverse(x), lim, arr), lim) GROUP BY [NULL, 1023, -2, NULL, 255, '0', NULL, 9223372036854775806] WITH ROLLUP; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, NO_COMMON_TYPE }
