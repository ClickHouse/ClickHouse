---
toc_priority: 46
toc_title: Arrays
---

# Functions for Working with Arrays {#functions-for-working-with-arrays}

## empty {#function-empty}

Returns 1 for an empty array, or 0 for a non-empty array.
The result type is UInt8.
The function also works for strings.

## notEmpty {#function-notempty}

Returns 0 for an empty array, or 1 for a non-empty array.
The result type is UInt8.
The function also works for strings.

## length {#array_functions-length}

Returns the number of items in the array.
The result type is UInt64.
The function also works for strings.

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32, emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

Accepts zero arguments and returns an empty array of the appropriate type.

## emptyArrayToSingle {#emptyarraytosingle}

Accepts an empty array and returns a one-element array that is equal to the default value.

## range(end), range(start, end \[, step\]) {#rangeend-rangestart-end-step}

Returns an array of numbers from start to end-1 by step.
If the argument `start` is not specified, defaults to 0.
If the argument `step` is not specified, defaults to 1.
It behaviors almost like pythonic `range`. But the difference is that all the arguments type must be `UInt` numbers.
Just in case, an exception is thrown if arrays with a total length of more than 100,000,000 elements are created in a data block.

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

Creates an array from the function arguments.
The arguments must be constants and have types that have the smallest common type. At least one argument must be passed, because otherwise it isn’t clear which type of array to create. That is, you can’t use this function to create an empty array (to do that, use the ‘emptyArray\*’ function described above).
Returns an ‘Array(T)’ type result, where ‘T’ is the smallest common type out of the passed arguments.

## arrayConcat {#arrayconcat}

Combines arrays passed as arguments.

``` sql
arrayConcat(arrays)
```

**Parameters**

-   `arrays` – Arbitrary number of arguments of [Array](../../sql-reference/data-types/array.md) type.
    **Example**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), operator arr\[n\] {#arrayelementarr-n-operator-arrn}

Get the element with the index `n` from the array `arr`. `n` must be any integer type.
Indexes in an array begin from one.
Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns some default value (0 for numbers, an empty string for strings, etc.), except for the case with a non-constant array and a constant index 0 (in this case there will be an error `Array indices are 1-based`).

## has(arr, elem) {#hasarr-elem}

Checks whether the ‘arr’ array has the ‘elem’ element.
Returns 0 if the the element is not in the array, or 1 if it is.

`NULL` is processed as a value.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## hasAll {#hasall}

Checks whether one array is a subset of another.

``` sql
hasAll(set, subset)
```

**Parameters**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**Return values**

-   `1`, if `set` contains all of the elements from `subset`.
-   `0`, otherwise.

**Peculiar properties**

-   An empty array is a subset of any array.
-   `Null` processed as a value.
-   Order of values in both of arrays doesn’t matter.

**Examples**

`SELECT hasAll([], [])` returns 1.

`SELECT hasAll([1, Null], [Null])` returns 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` returns 1.

`SELECT hasAll(['a', 'b'], ['a'])` returns 1.

`SELECT hasAll([1], ['a'])` returns 0.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` returns 0.

## hasAny {#hasany}

Checks whether two arrays have intersection by some elements.

``` sql
hasAny(array1, array2)
```

**Parameters**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**Return values**

-   `1`, if `array1` and `array2` have one similar element at least.
-   `0`, otherwise.

**Peculiar properties**

-   `Null` processed as a value.
-   Order of values in both of arrays doesn’t matter.

**Examples**

`SELECT hasAny([1], [])` returns `0`.

`SELECT hasAny([Null], [Null, 1])` returns `1`.

`SELECT hasAny([-128, 1., 512], [1])` returns `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` returns `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` returns `1`.

## hasSubstr {#hassubstr}

Checks whether all the elements of array2 appear in array1 in the same exact order. Therefore, the function will return 1, if and only if `array1 = prefix + array2 + suffix`.

``` sql
hasSubstr(array1, array2)
```

In other words, the functions will check whether all the elements of `array2` are contained in `array1` like
the `hasAll` function. In addition, it will check that the elements are observed in the same order in both `array1` and `array2`.

For Example:
- `hasSubstr([1,2,3,4], [2,3])` returns 1. However, `hasSubstr([1,2,3,4], [3,2])` will return `0`.
- `hasSubstr([1,2,3,4], [1,2,3])` returns 1. However, `hasSubstr([1,2,3,4], [1,2,4])` will return `0`.

**Parameters**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**Return values**

-   `1`, if `array1` contains `array2`.
-   `0`, otherwise.

**Peculiar properties**

-   The function will return `1` if `array2` is empty.
-   `Null` processed as a value. In other words `hasSubstr([1, 2, NULL, 3, 4], [2,3])` will return `0`. However, `hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` will return `1`
-   Order of values in both of arrays does matter.

**Examples**

`SELECT hasSubstr([], [])` returns 1.

`SELECT hasSubstr([1, Null], [Null])` returns 1.

`SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])` returns 0.

`SELECT hasSubstr(['a', 'b'], ['a'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])` returns 0.

`SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])` returns 1.

## indexOf(arr, x) {#indexofarr-x}

Returns the index of the first ‘x’ element (starting from 1) if it is in the array, or 0 if it is not.

Example:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

Elements set to `NULL` are handled as normal values.

## countEqual(arr, x) {#countequalarr-x}

Returns the number of elements in the array equal to x. Equivalent to arrayCount (elem -\> elem = x, arr).

`NULL` elements are handled as separate values.

Example:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## arrayEnumerate(arr) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

This function is normally used with ARRAY JOIN. It allows counting something just once for each array after applying ARRAY JOIN. Example:

``` sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

In this example, Reaches is the number of conversions (the strings received after applying ARRAY JOIN), and Hits is the number of pageviews (strings before ARRAY JOIN). In this particular case, you can get the same result in an easier way:

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

This function can also be used in higher-order functions. For example, you can use it to get array indexes for elements that match a condition.

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.
For example: arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

This function is useful when using ARRAY JOIN and aggregation of array elements.
Example:

``` sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

``` text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

In this example, each goal ID has a calculation of the number of conversions (each element in the Goals nested data structure is a goal that was reached, which we refer to as a conversion) and the number of sessions. Without ARRAY JOIN, we would have counted the number of sessions as sum(Sign). But in this particular case, the rows were multiplied by the nested Goals structure, so in order to count each session one time after this, we apply a condition to the value of the arrayEnumerateUniq(Goals.ID) function.

The arrayEnumerateUniq function can take multiple arrays of the same size as arguments. In this case, uniqueness is considered for tuples of elements in the same positions in all the arrays.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

This is necessary when using ARRAY JOIN with a nested data structure and further aggregation across multiple elements in this structure.

## arrayPopBack {#arraypopback}

Removes the last item from the array.

``` sql
arrayPopBack(array)
```

**Parameters**

-   `array` – Array.

**Example**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront {#arraypopfront}

Removes the first item from the array.

``` sql
arrayPopFront(array)
```

**Parameters**

-   `array` – Array.

**Example**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

Adds one item to the end of the array.

``` sql
arrayPushBack(array, single_value)
```

**Parameters**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array. For more information about the types of data in ClickHouse, see “[Data types](../../sql-reference/data-types/index.md#data_types)”. Can be `NULL`. The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable`.

**Example**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront {#arraypushfront}

Adds one element to the beginning of the array.

``` sql
arrayPushFront(array, single_value)
```

**Parameters**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array. For more information about the types of data in ClickHouse, see “[Data types](../../sql-reference/data-types/index.md#data_types)”. Can be `NULL`. The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable`.

**Example**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

Changes the length of the array.

``` sql
arrayResize(array, size[, extender])
```

**Parameters:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   If `size` is less than the original size of the array, the array is truncated from the right.
-   If `size` is larger than the initial size of the array, the array is extended to the right with `extender` values or default values for the data type of the array items.
-   `extender` — Value for extending an array. Can be `NULL`.

**Returned value:**

An array of length `size`.

**Examples of calls**

``` sql
SELECT arrayResize([1], 3)
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL)
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice {#arrayslice}

Returns a slice of the array.

``` sql
arraySlice(array, offset[, length])
```

**Parameters**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` - The length of the required slice. If you specify a negative value, the function returns an open slice `[offset, array_length - length)`. If you omit the value, the function returns the slice `[offset, the_end_of_array]`.

**Example**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

Array elements set to `NULL` are handled as normal values.

## arraySort(\[func,\] arr, …) {#array_functions-sort}

Sorts the elements of the `arr` array in ascending order. If the `func` function is specified, sorting order is determined by the result of the `func` function applied to the elements of the array. If `func` accepts multiple arguments, the `arraySort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arraySort` description.

Example of integer values sorting:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

Example of string values sorting:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

Consider the following sorting order for the `NULL`, `NaN` and `Inf` values:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` values are first in the array.
-   `NULL` values are last in the array.
-   `NaN` values are right before `NULL`.
-   `Inf` values are right before `NaN`.

Note that `arraySort` is a [higher-order function](../../sql-reference/functions/higher-order-functions.md). You can pass a lambda function to it as the first argument. In this case, sorting order is determined by the result of the lambda function applied to the elements of the array.

Let’s consider the following example:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` function sorts the keys in ascending order, the result is \[3, 2, 1\]. Thus, the `(x) –> -x` lambda function sets the [descending order](#array_functions-reverse-sort) in a sorting.

The lambda function can accept multiple arguments. In this case, you need to pass the `arraySort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Here, the elements that are passed in the second array (\[2, 1\]) define a sorting key for the corresponding element from the source array (\[‘hello’, ‘world’\]), that is, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn’t use `x`, actual values of the source array don’t affect the order in the result. So, ‘hello’ will be the second element in the result, and ‘world’ will be the first.

Other examples are shown below.

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

!!! note "Note"
    To improve sorting efficiency, the [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used.

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

Sorts the elements of the `arr` array in descending order. If the `func` function is specified, `arr` is sorted according to the result of the `func` function applied to the elements of the array, and then the sorted array is reversed. If `func` accepts multiple arguments, the `arrayReverseSort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arrayReverseSort` description.

Example of integer values sorting:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

Example of string values sorting:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

Consider the following sorting order for the `NULL`, `NaN` and `Inf` values:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` values are first in the array.
-   `NULL` values are last in the array.
-   `NaN` values are right before `NULL`.
-   `-Inf` values are right before `NaN`.

Note that the `arrayReverseSort` is a [higher-order function](../../sql-reference/functions/higher-order-functions.md). You can pass a lambda function to it as the first argument. Example is shown below.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

The array is sorted in the following way:

1.  At first, the source array (\[1, 2, 3\]) is sorted according to the result of the lambda function applied to the elements of the array. The result is an array \[3, 2, 1\].
2.  Array that is obtained on the previous step, is reversed. So, the final result is \[1, 2, 3\].

The lambda function can accept multiple arguments. In this case, you need to pass the `arrayReverseSort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

In this example, the array is sorted in the following way:

1.  At first, the source array (\[‘hello’, ‘world’\]) is sorted according to the result of the lambda function applied to the elements of the arrays. The elements that are passed in the second array (\[2, 1\]), define the sorting keys for corresponding elements from the source array. The result is an array \[‘world’, ‘hello’\].
2.  Array that was sorted on the previous step, is reversed. So, the final result is \[‘hello’, ‘world’\].

Other examples are shown below.

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

``` text
┌─res─────┐
│ [5,3,4] │
└─────────┘
```

``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

``` text
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayUniq(arr, …) {#arrayuniqarr}

If one argument is passed, it counts the number of different elements in the array.
If multiple arguments are passed, it counts the number of different tuples of elements at corresponding positions in multiple arrays.

If you want to get a list of unique items in an array, you can use arrayReduce(‘groupUniqArray’, arr).

## arrayJoin(arr) {#array-functions-join}

A special function. See the section [“ArrayJoin function”](../../sql-reference/functions/array-join.md#functions_arrayjoin).

## arrayDifference {#arraydifference}

Calculates the difference between adjacent array elements. Returns an array where the first element will be 0, the second is the difference between `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**Syntax**

``` sql
arrayDifference(array)
```

**Parameters**

-   `array` – [Array](https://clickhouse.tech/docs/en/data_types/array/).

**Returned values**

Returns an array of differences between adjacent elements.

Type: [UInt\*](https://clickhouse.tech/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.tech/docs/en/data_types/int_uint/#int-ranges), [Float\*](https://clickhouse.tech/docs/en/data_types/float/).

**Example**

Query:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

Result:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

Example of the overflow due to result type Int64:

Query:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

Result:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct {#arraydistinct}

Takes an array, returns an array containing the distinct elements only.

**Syntax**

``` sql
arrayDistinct(array)
```

**Parameters**

-   `array` – [Array](https://clickhouse.tech/docs/en/data_types/array/).

**Returned values**

Returns an array containing the distinct elements.

**Example**

Query:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

Result:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense(arr) {#array_functions-arrayenumeratedense}

Returns an array of the same size as the source array, indicating where each element first appears in the source array.

Example:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## arrayIntersect(arr) {#array-functions-arrayintersect}

Takes multiple arrays, returns an array with elements that are present in all source arrays. Elements order in the resulting array is the same as in the first array.

Example:

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

``` text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayReduce {#arrayreduce}

Applies an aggregate function to array elements and returns its result. The name of the aggregation function is passed as a string in single quotes `'max'`, `'sum'`. When using parametric aggregate functions, the parameter is indicated after the function name in parentheses `'uniqUpTo(6)'`.

**Syntax**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Parameters**

-   `agg_func` — The name of an aggregate function which should be a constant [string](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [array](../../sql-reference/data-types/array.md) type columns as the parameters of the aggregation function.

**Returned value**

**Example**

Query:

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

Result:

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

If an aggregate function takes multiple arguments, then this function must be applied to multiple arrays of the same size.

Query:

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

Result:

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

Example with a parametric aggregate function:

Query:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

Result:

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceInRanges {#arrayreduceinranges}

Applies an aggregate function to array elements in given ranges and returns an array containing the result corresponding to each range. The function will return the same result as multiple `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**Syntax**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**Parameters**

-   `agg_func` — The name of an aggregate function which should be a constant [string](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [array](../../sql-reference/data-types/array.md) of [tuples](../../sql-reference/data-types/tuple.md) which containing the index and the length of each range.
-   `arr` — Any number of [Array](../../sql-reference/data-types/array.md) type columns as the parameters of the aggregation function.

**Returned value**

-   Array containing results of the aggregate function over specified ranges.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

Result:

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## arrayReverse(arr) {#arrayreverse}

Returns an array of the same size as the original array containing the elements in reverse order.

Example:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## reverse(arr) {#array-functions-reverse}

Synonym for [“arrayReverse”](#arrayreverse)

## arrayFlatten {#arrayflatten}

Converts an array of arrays to a flat array.

Function:

-   Applies to any depth of nested arrays.
-   Does not change arrays that are already flat.

The flattened array contains all the elements from all source arrays.

**Syntax**

``` sql
flatten(array_of_arrays)
```

Alias: `flatten`.

**Parameters**

-   `array_of_arrays` — [Array](../../sql-reference/data-types/array.md) of arrays. For example, `[[1,2,3], [4,5]]`.

**Examples**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact {#arraycompact}

Removes consecutive duplicate elements from an array. The order of result values is determined by the order in the source array.

**Syntax**

``` sql
arrayCompact(arr)
```

**Parameters**

`arr` — The [array](../../sql-reference/data-types/array.md) to inspect.

**Returned value**

The array without duplicate.

Type: `Array`.

**Example**

Query:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

Result:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

Combines multiple arrays into a single array. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.

**Syntax**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**Parameters**

-   `arrN` — [Array](../../sql-reference/data-types/array.md).

The function can take any number of arrays of different types. All the input arrays must be of equal size.

**Returned value**

-   Array with elements from the source arrays grouped into [tuples](../../sql-reference/data-types/tuple.md). Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

Result:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayAUC {#arrayauc}

Calculate AUC (Area Under the Curve, which is a concept in machine learning, see more details: https://en.wikipedia.org/wiki/Receiver\_operating\_characteristic\#Area\_under\_the\_curve).

**Syntax**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**Parameters**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**Returned value**
Returns AUC value with type Float64.

**Example**
Query:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

Result:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
