-- The elements of a state of `groupUniqArray` over a fixed-size type are inserted with
-- `insertData`, which reads the whole width of the value and ignores the length it is given, so a
-- shorter element of a crafted state would read past the end of the buffer.

SELECT groupUniqArrayMerge(CAST(unhex('010141') AS AggregateFunction(groupUniqArray, Decimal256(0)))); -- { serverError INCORRECT_DATA }
SELECT groupUniqArrayMerge(CAST(unhex('02014120' || repeat('AA', 32)) AS AggregateFunction(groupUniqArray, Decimal256(0)))); -- { serverError INCORRECT_DATA }

SELECT arraySort(groupUniqArrayMerge(state)) FROM (SELECT groupUniqArrayState(number % 3) AS state FROM numbers(10));
SELECT arraySort(groupUniqArrayMerge(state)) FROM (SELECT groupUniqArrayState(toDecimal256(number % 3, 0)) AS state FROM numbers(10));
