---
slug: /en/sql-reference/functions/array-functions
sidebar_position: 10
sidebar_label: Arrays
---

# Array Functions

## empty {#empty}

Checks whether the input array is empty.

**Syntax**

``` sql
empty([x])
```

An array is considered empty if it does not contain any elements.

:::note
Can be optimized by enabling the [`optimize_functions_to_subcolumns` setting](../../operations/settings/settings.md#optimize-functions-to-subcolumns). With `optimize_functions_to_subcolumns = 1` the function reads only [size0](../data-types/array.md#array-size) subcolumn instead of reading and processing the whole array column. The query `SELECT empty(arr) FROM TABLE;` transforms to `SELECT arr.size0 = 0 FROM TABLE;`.
:::

The function also works for [strings](string-functions.md#empty) or [UUID](uuid-functions.md#empty).

**Arguments**

- `[x]` — Input array. [Array](../data-types/array.md).

**Returned value**

- Returns `1` for an empty array or `0` for a non-empty array. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT empty([]);
```

Result:

```text
┌─empty(array())─┐
│              1 │
└────────────────┘
```

## notEmpty {#notempty}

Checks whether the input array is non-empty.

**Syntax**

``` sql
notEmpty([x])
```

An array is considered non-empty if it contains at least one element.

:::note
Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [size0](../data-types/array.md#array-size) subcolumn instead of reading and processing the whole array column. The query `SELECT notEmpty(arr) FROM table` transforms to `SELECT arr.size0 != 0 FROM TABLE`.
:::

The function also works for [strings](string-functions.md#notempty) or [UUID](uuid-functions.md#notempty).

**Arguments**

- `[x]` — Input array. [Array](../data-types/array.md).

**Returned value**

- Returns `1` for a non-empty array or `0` for an empty array. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT notEmpty([1,2]);
```

Result:

```text
┌─notEmpty([1, 2])─┐
│                1 │
└──────────────────┘
```

## length

Returns the number of items in the array.
The result type is UInt64.
The function also works for strings.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [size0](../data-types/array.md#array-size) subcolumn instead of reading and processing the whole array column. The query `SELECT length(arr) FROM table` transforms to `SELECT arr.size0 FROM TABLE`.

Alias: `OCTET_LENGTH`

## emptyArrayUInt8

Returns an empty UInt8 array.

**Syntax**

```sql
emptyArrayUInt8()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayUInt8();
```

Result:

```response
[]
```

## emptyArrayUInt16

Returns an empty UInt16 array.

**Syntax**

```sql
emptyArrayUInt16()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayUInt16();

```

Result:

```response
[]
```

## emptyArrayUInt32

Returns an empty UInt32 array.

**Syntax**

```sql
emptyArrayUInt32()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayUInt32();
```

Result:

```response
[]
```

## emptyArrayUInt64

Returns an empty UInt64 array.

**Syntax**

```sql
emptyArrayUInt64()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayUInt64();
```

Result:

```response
[]
```

## emptyArrayInt8

Returns an empty Int8 array.

**Syntax**

```sql
emptyArrayInt8()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayInt8();
```

Result:

```response
[]
```

## emptyArrayInt16

Returns an empty Int16 array.

**Syntax**

```sql
emptyArrayInt16()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayInt16();
```

Result:

```response
[]
```

## emptyArrayInt32

Returns an empty Int32 array.

**Syntax**

```sql
emptyArrayInt32()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayInt32();
```

Result:

```response
[]
```

## emptyArrayInt64

Returns an empty Int64 array.

**Syntax**

```sql
emptyArrayInt64()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayInt64();
```

Result:

```response
[]
```

## emptyArrayFloat32 

Returns an empty Float32 array.

**Syntax**

```sql
emptyArrayFloat32()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayFloat32();
```

Result:

```response
[]
```

## emptyArrayFloat64

Returns an empty Float64 array.

**Syntax**

```sql
emptyArrayFloat64()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayFloat64();
```

Result:

```response
[]
```

## emptyArrayDate

Returns an empty Date array.

**Syntax**

```sql
emptyArrayDate()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayDate();
```

## emptyArrayDateTime

Returns an empty DateTime array.

**Syntax**

```sql
[]
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayDateTime();
```

Result:

```response
[]
```

## emptyArrayString

Returns an empty String array.

**Syntax**

```sql
emptyArrayString()
```

**Arguments**

None.

**Returned value**

An empty array.

**Examples**

Query:

```sql
SELECT emptyArrayString();
```

Result:

```response
[]
```

## emptyArrayToSingle

Accepts an empty array and returns a one-element array that is equal to the default value.

## range(end), range(\[start, \] end \[, step\])

Returns an array of numbers from `start` to `end - 1` by `step`. The supported types are [UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../data-types/int-uint.md).

**Syntax**

``` sql
range([start, ] end [, step])
```

**Arguments**

- `start` — The first element of the array. Optional, required if `step` is used. Default value: 0.
- `end` — The number before which the array is constructed. Required.
- `step` — Determines the incremental step between each element in the array. Optional. Default value: 1.

**Returned value**

- Array of numbers from `start` to `end - 1` by `step`.

**Implementation details**

- All arguments `start`, `end`, `step` must be below data types: `UInt8`, `UInt16`, `UInt32`, `UInt64`,`Int8`, `Int16`, `Int32`, `Int64`, as well as elements of the returned array, which's type is a super type of all arguments.
- An exception is thrown if query results in arrays with a total length of more than number of elements specified by the [function_range_max_elements_in_block](../../operations/settings/settings.md#function_range_max_elements_in_block) setting.
- Returns Null if any argument has Nullable(Nothing) type. An exception is thrown if any argument has Null value (Nullable(T) type).

**Examples**

Query:

``` sql
SELECT range(5), range(1, 5), range(1, 5, 2), range(-1, 5, 2);
```

Result:

```txt
┌─range(5)────┬─range(1, 5)─┬─range(1, 5, 2)─┬─range(-1, 5, 2)─┐
│ [0,1,2,3,4] │ [1,2,3,4]   │ [1,3]          │ [-1,1,3]        │
└─────────────┴─────────────┴────────────────┴─────────────────┘
```

## array(x1, ...), operator \[x1, ...\]

Creates an array from the function arguments.
The arguments must be constants and have types that have the smallest common type. At least one argument must be passed, because otherwise it isn’t clear which type of array to create. That is, you can’t use this function to create an empty array (to do that, use the ‘emptyArray\*’ function described above).
Returns an ‘Array(T)’ type result, where ‘T’ is the smallest common type out of the passed arguments.

## arrayWithConstant(length, elem)

Creates an array of length `length` filled with the constant `elem`.

## arrayConcat

Combines arrays passed as arguments.

``` sql
arrayConcat(arrays)
```

**Arguments**

- `arrays` – Arbitrary number of arguments of [Array](../data-types/array.md) type.

**Example**

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), operator arr\[n\]

Get the element with the index `n` from the array `arr`. `n` must be any integer type.
Indexes in an array begin from one.

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns some default value (0 for numbers, an empty string for strings, etc.), except for the case with a non-constant array and a constant index 0 (in this case there will be an error `Array indices are 1-based`).

## has(arr, elem)

Checks whether the ‘arr’ array has the ‘elem’ element.
Returns 0 if the element is not in the array, or 1 if it is.

`NULL` is processed as a value.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## arrayElementOrNull(arr, n)

Get the element with the index `n`from the array `arr`. `n` must be any integer type.
Indexes in an array begin from one.

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns `NULL` instead of a default value.

### Examples

``` sql
SELECT arrayElementOrNull([1, 2, 3], 2), arrayElementOrNull([1, 2, 3], 4)
```

``` text
 ┌─arrayElementOrNull([1, 2, 3], 2)─┬─arrayElementOrNull([1, 2, 3], 4)─┐
 │                                2 │                             ᴺᵁᴸᴸ │
 └──────────────────────────────────┴──────────────────────────────────┘
```

## hasAll {#hasall}

Checks whether one array is a subset of another.

``` sql
hasAll(set, subset)
```

**Arguments**

- `set` – Array of any type with a set of elements.
- `subset` – Array of any type that shares a common supertype with `set` containing elements that should be tested to be a subset of `set`.

**Return values**

- `1`, if `set` contains all of the elements from `subset`.
- `0`, otherwise.

Raises an exception `NO_COMMON_TYPE` if the set and subset elements do not share a common supertype.

**Peculiar properties**

- An empty array is a subset of any array.
- `Null` processed as a value.
- Order of values in both of arrays does not matter.

**Examples**

`SELECT hasAll([], [])` returns 1.

`SELECT hasAll([1, Null], [Null])` returns 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` returns 1.

`SELECT hasAll(['a', 'b'], ['a'])` returns 1.

`SELECT hasAll([1], ['a'])` raises a `NO_COMMON_TYPE` exception.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` returns 0.

## hasAny {#hasany}

Checks whether two arrays have intersection by some elements.

``` sql
hasAny(array1, array2)
```

**Arguments**

- `array1` – Array of any type with a set of elements.
- `array2` – Array of any type that shares a common supertype with `array1`.

**Return values**

- `1`, if `array1` and `array2` have one similar element at least.
- `0`, otherwise.

Raises an exception `NO_COMMON_TYPE` if the array1 and array2 elements do not share a common supertype.

**Peculiar properties**

- `Null` processed as a value.
- Order of values in both of arrays does not matter.

**Examples**

`SELECT hasAny([1], [])` returns `0`.

`SELECT hasAny([Null], [Null, 1])` returns `1`.

`SELECT hasAny([-128, 1., 512], [1])` returns `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` raises a `NO_COMMON_TYPE` exception.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` returns `1`.

## hasSubstr

Checks whether all the elements of array2 appear in array1 in the same exact order. Therefore, the function will return 1, if and only if `array1 = prefix + array2 + suffix`.

``` sql
hasSubstr(array1, array2)
```

In other words, the functions will check whether all the elements of `array2` are contained in `array1` like
the `hasAll` function. In addition, it will check that the elements are observed in the same order in both `array1` and `array2`.

For Example:

- `hasSubstr([1,2,3,4], [2,3])` returns 1. However, `hasSubstr([1,2,3,4], [3,2])` will return `0`.
- `hasSubstr([1,2,3,4], [1,2,3])` returns 1. However, `hasSubstr([1,2,3,4], [1,2,4])` will return `0`.

**Arguments**

- `array1` – Array of any type with a set of elements.
- `array2` – Array of any type with a set of elements.

**Return values**

- `1`, if `array1` contains `array2`.
- `0`, otherwise.

Raises an exception `NO_COMMON_TYPE` if the array1 and array2 elements do not share a common supertype.

**Peculiar properties**

- The function will return `1` if `array2` is empty.
- `Null` processed as a value. In other words `hasSubstr([1, 2, NULL, 3, 4], [2,3])` will return `0`. However, `hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` will return `1`
- Order of values in both of arrays does matter.

**Examples**

`SELECT hasSubstr([], [])` returns 1.

`SELECT hasSubstr([1, Null], [Null])` returns 1.

`SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])` returns 0.

`SELECT hasSubstr(['a', 'b'], ['a'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])` returns 0.

`SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])` returns 1.
i
`SELECT hasSubstr([1, 2, NULL, 3, 4], ['a'])` raises a `NO_COMMON_TYPE` exception.


## indexOf(arr, x)

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

## arrayCount(\[func,\] arr1, ...)

Returns the number of elements for which `func(arr1[i], ..., arrN[i])` returns something other than 0. If `func` is not specified, it returns the number of non-zero elements in the array.

Note that the `arrayCount` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

## arrayDotProduct

Returns the dot product of two arrays.

**Syntax**

```sql
arrayDotProduct(vector1, vector2)
```

Alias: `scalarProduct`, `dotProduct`

**Parameters**

- `vector1`: First vector. [Array](../data-types/array.md) or [Tuple](../data-types/tuple.md) of numeric values.
- `vector2`: Second vector. [Array](../data-types/array.md) or [Tuple](../data-types/tuple.md) of numeric values.

:::note
The sizes of the two vectors must be equal. Arrays and Tuples may also contain mixed element types.
:::

**Returned value**

- The dot product of the two vectors. [Numeric](https://clickhouse.com/docs/en/native-protocol/columns#numeric-types).

:::note
The return type is determined by the type of the arguments. If Arrays or Tuples contain mixed element types then the result type is the supertype.
:::

**Examples**

Query:

```sql
SELECT arrayDotProduct([1, 2, 3], [4, 5, 6]) AS res, toTypeName(res);
```

Result:

```response
32	UInt16
```

Query:

```sql
SELECT dotProduct((1::UInt16, 2::UInt8, 3::Float32),(4::Int16, 5::Float32, 6::UInt8)) AS res, toTypeName(res);
```

Result:

```response
32	Float64
```

## countEqual(arr, x)

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

## arrayEnumerate(arr)

Returns the array \[1, 2, 3, ..., length (arr) \]

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

## arrayEnumerateUniq(arr, ...)

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

## arrayEnumerateUniqRanked

Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value. It allows for enumeration of a multidimensional array with the ability to specify how deep to look inside the array.

**Syntax**

```sql
arrayEnumerateUniqRanked(clear_depth, arr, max_array_depth)
```

**Parameters**

- `clear_depth`: Enumerate elements at the specified level separately. Positive [Integer](../data-types/int-uint.md) less than or equal to `max_arr_depth`.
- `arr`: N-dimensional array to enumerate. [Array](../data-types/array.md).
- `max_array_depth`: The maximum effective depth. Positive [Integer](../data-types/int-uint.md) less than or equal to the depth of `arr`.

**Example**

With `clear_depth=1` and `max_array_depth=1`, the result of `arrayEnumerateUniqRanked` is identical to that which [`arrayEnumerateUniq`](#arrayenumerateuniqarr) would give for the same array.

Query:

``` sql
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1);
```

Result:

``` text
[1,1,2]
```

In this example, `arrayEnumerateUniqRanked` is used to obtain an array indicating, for each element of the multidimensional array, what its position is among elements of the same value. For the first row of the passed array,`[1,2,3]`, the corresponding result is `[1,1,1]`, indicating that this is the first time `1`,`2` and `3` are encountered. For the second row of the provided array,`[2,2,1]`, the corresponding result is `[2,3,3]`, indicating that `2` is encountered for a second and third time, and `1` is encountered for the second time. Likewise, for the third row of the provided array `[3]` the corresponding result is `[2]` indicating that `3` is encountered for the second time. 

Query:

``` sql
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2);
```

Result:

``` text
[[1,1,1],[2,3,2],[2]]
```

Changing `clear_depth=2`, results in elements being enumerated separately for each row.

Query:

``` sql
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2);
```

Result:

``` text
[[1,1,1],[1,2,1],[1]]
```

## arrayPopBack

Removes the last item from the array.

``` sql
arrayPopBack(array)
```

**Arguments**

- `array` – Array.

**Example**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res;
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront

Removes the first item from the array.

``` sql
arrayPopFront(array)
```

**Arguments**

- `array` – Array.

**Example**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res;
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack

Adds one item to the end of the array.

``` sql
arrayPushBack(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array. For more information about the types of data in ClickHouse, see “[Data types](../data-types/index.md#data_types)”. Can be `NULL`. The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable`.

**Example**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res;
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront

Adds one element to the beginning of the array.

``` sql
arrayPushFront(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array. For more information about the types of data in ClickHouse, see “[Data types](../data-types/index.md#data_types)”. Can be `NULL`. The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable`.

**Example**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res;
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize

Changes the length of the array.

``` sql
arrayResize(array, size[, extender])
```

**Arguments:**

- `array` — Array.
- `size` — Required length of the array.
  - If `size` is less than the original size of the array, the array is truncated from the right.
- If `size` is larger than the initial size of the array, the array is extended to the right with `extender` values or default values for the data type of the array items.
- `extender` — Value for extending an array. Can be `NULL`.

**Returned value:**

An array of length `size`.

**Examples of calls**

``` sql
SELECT arrayResize([1], 3);
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL);
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice

Returns a slice of the array.

``` sql
arraySlice(array, offset[, length])
```

**Arguments**

- `array` – Array of data.
- `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
- `length` – The length of the required slice. If you specify a negative value, the function returns an open slice `[offset, array_length - length]`. If you omit the value, the function returns the slice `[offset, the_end_of_array]`.

**Example**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res;
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

Array elements set to `NULL` are handled as normal values.

## arrayShingles

Generates an array of "shingles", i.e. consecutive sub-arrays with specified length of the input array.

**Syntax**

``` sql
arrayShingles(array, length)
```

**Arguments**

- `array` — Input array [Array](../data-types/array.md).
- `length` — The length of each shingle.

**Returned value**

- An array of generated shingles. [Array](../data-types/array.md).

**Examples**

Query:

``` sql
SELECT arrayShingles([1,2,3,4], 3) as res;
```

Result:

``` text
┌─res───────────────┐
│ [[1,2,3],[2,3,4]] │
└───────────────────┘
```

## arraySort(\[func,\] arr, ...) {#sort}

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

- `-Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL`.
- `Inf` values are right before `NaN`.

Note that `arraySort` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument. In this case, sorting order is determined by the result of the lambda function applied to the elements of the array.

Let’s consider the following example:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` function sorts the keys in ascending order, the result is \[3, 2, 1\]. Thus, the `(x) –> -x` lambda function sets the [descending order](#arrayreversesort) in a sorting.

The lambda function can accept multiple arguments. In this case, you need to pass the `arraySort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Here, the elements that are passed in the second array (\[2, 1\]) define a sorting key for the corresponding element from the source array (\[‘hello’, ‘world’\]), that is, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function does not use `x`, actual values of the source array do not affect the order in the result. So, ‘hello’ will be the second element in the result, and ‘world’ will be the first.

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

:::note
To improve sorting efficiency, the [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used.
:::

## arrayPartialSort(\[func,\] limit, arr, ...)

Same as `arraySort` with additional `limit` argument allowing partial sorting. Returns an array of the same size as the original array where elements in range `[1..limit]` are sorted in ascending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.

## arrayReverseSort

Sorts the elements of the `arr` array in descending order. If the `func` function is specified, `arr` is sorted according to the result of the `func` function applied to the elements of the array, and then the sorted array is reversed. If `func` accepts multiple arguments, the `arrayReverseSort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arrayReverseSort` description.

**Syntax**

```sql
arrayReverseSort([func,] arr, ...)
```
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

- `Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL`.
- `-Inf` values are right before `NaN`.

Note that the `arrayReverseSort` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument. Example is shown below.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

The array is sorted in the following way:

1. At first, the source array (\[1, 2, 3\]) is sorted according to the result of the lambda function applied to the elements of the array. The result is an array \[3, 2, 1\].
2. Array that is obtained on the previous step, is reversed. So, the final result is \[1, 2, 3\].

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

1. At first, the source array (\[‘hello’, ‘world’\]) is sorted according to the result of the lambda function applied to the elements of the arrays. The elements that are passed in the second array (\[2, 1\]), define the sorting keys for corresponding elements from the source array. The result is an array \[‘world’, ‘hello’\].
2. Array that was sorted on the previous step, is reversed. So, the final result is \[‘hello’, ‘world’\].

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

## arrayPartialReverseSort(\[func,\] limit, arr, ...)

Same as `arrayReverseSort` with additional `limit` argument allowing partial sorting. Returns an array of the same size as the original array where elements in range `[1..limit]` are sorted in descending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.

## arrayShuffle

Returns an array of the same size as the original array containing the elements in shuffled order.
Elements are reordered in such a way that each possible permutation of those elements has equal probability of appearance.

**Syntax**

```sql
arrayShuffle(arr[, seed])
```

**Parameters**

- `arr`: The array to partially shuffle. [Array](../data-types/array.md).
- `seed` (optional): seed to be used with random number generation. If not provided a random one is used. [UInt or Int](../data-types/int-uint.md).

**Returned value**

- Array with elements shuffled.

**Implementation details**

:::note 
This function will not materialize constants.
:::

**Examples**

In this example, `arrayShuffle` is used without providing a `seed` and will therefore generate one randomly itself. 

Query:

```sql
SELECT arrayShuffle([1, 2, 3, 4]);
```

Note: when using [ClickHouse Fiddle](https://fiddle.clickhouse.com/), the exact response may differ due to random nature of the function.

Result: 

```response
[1,4,2,3]
```

In this example, `arrayShuffle` is provided a `seed` and will produce stable results.

Query:

```sql
SELECT arrayShuffle([1, 2, 3, 4], 41);
```

Result: 

```response
[3,2,1,4]
```

## arrayPartialShuffle

Given an input array of cardinality `N`, returns an array of size N where elements in the range `[1...limit]` are shuffled and the remaining elements in the range `(limit...n]` are unshuffled.

**Syntax**

```sql
arrayPartialShuffle(arr[, limit[, seed]])
```

**Parameters**

- `arr`: The array size `N` to partially shuffle. [Array](../data-types/array.md).
- `limit` (optional): The number to limit element swaps to, in the range `[1..N]`. [UInt or Int](../data-types/int-uint.md).
- `seed` (optional): The seed value to be used with random number generation. If not provided a random one is used. [UInt or Int](../data-types/int-uint.md)

**Returned value**

- Array with elements partially shuffled.

**Implementation details**

:::note 
This function will not materialize constants.

The value of `limit` should be in the range `[1..N]`. Values outside of that range are equivalent to performing full [arrayShuffle](#arrayshuffle).
:::

**Examples**

Note: when using [ClickHouse Fiddle](https://fiddle.clickhouse.com/), the exact response may differ due to random nature of the function. 

Query:

```sql
SELECT arrayPartialShuffle([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1)
```

Result:

The order of elements is preserved (`[2,3,4,5], [7,8,9,10]`) except for the two shuffled elements `[1, 6]`. No `seed` is provided so the function selects its own randomly.

```response
[6,2,3,4,5,1,7,8,9,10]
```

In this example, the `limit` is increased to `2` and a `seed` value is provided. The order 

Query:

```sql
SELECT arrayPartialShuffle([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2);
```

The order of elements is preserved (`[4, 5, 6, 7, 8], [10]`) except for the four shuffled elements `[1, 2, 3, 9]`.

Result: 
```response
[3,9,1,4,5,6,7,8,2,10]
```

## arrayUniq(arr, ...)

If one argument is passed, it counts the number of different elements in the array.
If multiple arguments are passed, it counts the number of different tuples of elements at corresponding positions in multiple arrays.

If you want to get a list of unique items in an array, you can use arrayReduce(‘groupUniqArray’, arr).

## arrayJoin(arr)

A special function. See the section [“ArrayJoin function”](../../sql-reference/functions/array-join.md#functions_arrayjoin).

## arrayDifference

Calculates an array of differences between adjacent array elements. The first element of the result array will be 0, the second `a[1] - a[0]`, the third `a[2] - a[1]`, etc. The type of elements in the result array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**Syntax**

``` sql
arrayDifference(array)
```

**Arguments**

- `array` – [Array](https://clickhouse.com/docs/en/data_types/array/).

**Returned values**

Returns an array of differences between adjacent array elements. [UInt\*](https://clickhouse.com/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.com/docs/en/data_types/int_uint/#int-ranges), [Float\*](https://clickhouse.com/docs/en/data_types/float/).

**Example**

Query:

``` sql
SELECT arrayDifference([1, 2, 3, 4]);
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
SELECT arrayDifference([0, 10000000000000000000]);
```

Result:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct

Takes an array, returns an array containing the distinct elements only.

**Syntax**

``` sql
arrayDistinct(array)
```

**Arguments**

- `array` – [Array](https://clickhouse.com/docs/en/data_types/array/).

**Returned values**

Returns an array containing the distinct elements.

**Example**

Query:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1]);
```

Result:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense

Returns an array of the same size as the source array, indicating where each element first appears in the source array.

**Syntax**

```sql
arrayEnumerateDense(arr)
```

**Example**

Query:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

Result:

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```
## arrayEnumerateDenseRanked

Returns an array the same size as the source array, indicating where each element first appears in the source array. It allows for enumeration of a multidimensional array with the ability to specify how deep to look inside the array.

**Syntax**

```sql
arrayEnumerateDenseRanked(clear_depth, arr, max_array_depth)
```

**Parameters**

- `clear_depth`: Enumerate elements at the specified level separately. Positive [Integer](../data-types/int-uint.md) less than or equal to `max_arr_depth`.
- `arr`: N-dimensional array to enumerate. [Array](../data-types/array.md).
- `max_array_depth`: The maximum effective depth. Positive [Integer](../data-types/int-uint.md) less than or equal to the depth of `arr`.

**Example**

With `clear_depth=1` and `max_array_depth=1`, the result is identical to what [arrayEnumerateDense](#arrayenumeratedense) would give.

Query:

``` sql
SELECT arrayEnumerateDenseRanked(1,[10, 20, 10, 30],1);
```

Result:

``` text
[1,2,1,3]
```

In this example, `arrayEnumerateDenseRanked` is used to obtain an array indicating, for each element of the multidimensional array, what its position is among elements of the same value. For the first row of the passed array,`[10,10,30,20]`, the corresponding first row of the result is `[1,1,2,3]`, indicating that `10` is the first number encountered in position 1 and 2, `30` the second number encountered in position 3 and `20` is the third number encountered in position 4. For the second row, `[40, 50, 10, 30]`, the corresponding second row of the result is `[4,5,1,2]`, indicating that `40` and `50` are the fourth and fifth numbers encountered in position 1 and 2 of that row, that another `10` (the first encountered number) is in position 3 and `30` (the second number encountered) is in the last position. 


Query:

``` sql
SELECT arrayEnumerateDenseRanked(1,[[10,10,30,20],[40,50,10,30]],2);
```

Result:

``` text
[[1,1,2,3],[4,5,1,2]]
```

Changing `clear_depth=2` results in the enumeration occurring separately for each row anew.

Query:

``` sql
SELECT arrayEnumerateDenseRanked(2,[[10,10,30,20],[40,50,10,30]],2);
```

Result:

``` text
[[1,1,2,3],[1,2,3,4]]
```

## arrayUnion(arr)

Takes multiple arrays, returns an array that contains all elements that are present in any of the source arrays.

Example:
```sql
SELECT
    arrayUnion([-2, 1], [10, 1], [-2], []) as num_example,
    arrayUnion(['hi'], [], ['hello', 'hi']) as str_example,
    arrayUnion([1, 3, NULL], [2, 3, NULL]) as null_example
```

```text
┌─num_example─┬─str_example────┬─null_example─┐
│ [10,-2,1]   │ ['hello','hi'] │ [3,2,1,NULL] │
└─────────────┴────────────────┴──────────────┘
```

## arrayIntersect(arr)

Takes multiple arrays, returns an array with elements that are present in all source arrays.

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

## arrayJaccardIndex

Returns the [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index) of two arrays.

**Example**

Query:
``` sql
SELECT arrayJaccardIndex([1, 2], [2, 3]) AS res
```

Result:
``` text
┌─res────────────────┐
│ 0.3333333333333333 │
└────────────────────┘
```

## arrayReduce

Applies an aggregate function to array elements and returns its result. The name of the aggregation function is passed as a string in single quotes `'max'`, `'sum'`. When using parametric aggregate functions, the parameter is indicated after the function name in parentheses `'uniqUpTo(6)'`.

**Syntax**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Arguments**

- `agg_func` — The name of an aggregate function which should be a constant [string](../data-types/string.md).
- `arr` — Any number of [array](../data-types/array.md) type columns as the parameters of the aggregation function.

**Returned value**

**Example**

Query:

``` sql
SELECT arrayReduce('max', [1, 2, 3]);
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
SELECT arrayReduce('maxIf', [3, 5], [1, 0]);
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
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
```

Result:

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

**See also**

- [arrayFold](#arrayfold)

## arrayReduceInRanges

Applies an aggregate function to array elements in given ranges and returns an array containing the result corresponding to each range. The function will return the same result as multiple `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**Syntax**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**Arguments**

- `agg_func` — The name of an aggregate function which should be a constant [string](../data-types/string.md).
- `ranges` — The ranges to aggretate which should be an [array](../data-types/array.md) of [tuples](../data-types/tuple.md) which containing the index and the length of each range.
- `arr` — Any number of [Array](../data-types/array.md) type columns as the parameters of the aggregation function.

**Returned value**

- Array containing results of the aggregate function over specified ranges. [Array](../data-types/array.md).

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

## arrayFold

Applies a lambda function to one or more equally-sized arrays and collects the result in an accumulator.

**Syntax**

``` sql
arrayFold(lambda_function, arr1, arr2, ..., accumulator)
```

**Example**

Query:

``` sql
SELECT arrayFold( acc,x -> acc + x*2,  [1, 2, 3, 4], toInt64(3)) AS res;
```

Result:

``` text
┌─res─┐
│  23 │
└─────┘
```

**Example with the Fibonacci sequence**

```sql
SELECT arrayFold( acc,x -> (acc.2, acc.2 + acc.1), range(number), (1::Int64, 0::Int64)).1 AS fibonacci
FROM numbers(1,10);

┌─fibonacci─┐
│         0 │
│         1 │
│         1 │
│         2 │
│         3 │
│         5 │
│         8 │
│        13 │
│        21 │
│        34 │
└───────────┘
```

**See also**

- [arrayReduce](#arrayreduce)

## arrayReverse

Returns an array of the same size as the original array containing the elements in reverse order.

**Syntax**

```sql
arrayReverse(arr)
```

Example:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## reverse(arr)

Synonym for [“arrayReverse”](#arrayreverse)

## arrayFlatten

Converts an array of arrays to a flat array.

Function:

- Applies to any depth of nested arrays.
- Does not change arrays that are already flat.

The flattened array contains all the elements from all source arrays.

**Syntax**

``` sql
flatten(array_of_arrays)
```

Alias: `flatten`.

**Parameters**

- `array_of_arrays` — [Array](../data-types/array.md) of arrays. For example, `[[1,2,3], [4,5]]`.

**Examples**

``` sql
SELECT flatten([[[1]], [[2], [3]]]);
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact

Removes consecutive duplicate elements from an array. The order of result values is determined by the order in the source array.

**Syntax**

``` sql
arrayCompact(arr)
```

**Arguments**

`arr` — The [array](../data-types/array.md) to inspect.

**Returned value**

The array without duplicate. [Array](../data-types/array.md).

**Example**

Query:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3]);
```

Result:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip

Combines multiple arrays into a single array. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.

**Syntax**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**Arguments**

- `arrN` — [Array](../data-types/array.md).

The function can take any number of arrays of different types. All the input arrays must be of equal size.

**Returned value**

- Array with elements from the source arrays grouped into [tuples](../data-types/tuple.md). Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed. [Array](../data-types/array.md).

**Example**

Query:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1]);
```


Result:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayZipUnaligned

Combines multiple arrays into a single array, allowing for unaligned arrays. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.

**Syntax**

``` sql
arrayZipUnaligned(arr1, arr2, ..., arrN)
```

**Arguments**

- `arrN` — [Array](../data-types/array.md).

The function can take any number of arrays of different types.

**Returned value**

- Array with elements from the source arrays grouped into [tuples](../data-types/tuple.md). Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed. [Array](../data-types/array.md). If the arrays have different sizes, the shorter arrays will be padded with `null` values.

**Example**

Query:

``` sql
SELECT arrayZipUnaligned(['a'], [1, 2, 3]);
```

Result:

``` text
┌─arrayZipUnaligned(['a'], [1, 2, 3])─┐
│ [('a',1),(NULL,2),(NULL,3)]         │
└─────────────────────────────────────┘
```


## arrayAUC

Calculate AUC (Area Under the Curve, which is a concept in machine learning, see more details: <https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve>).

**Syntax**

``` sql
arrayAUC(arr_scores, arr_labels[, scale])
```

**Arguments**

- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negative sample.
- `scale` - Optional. Wether to return the normalized area. Default value: true. [Bool]

**Returned value**

Returns AUC value with type Float64.

**Example**

Query:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
```

Result:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└───────────────────────────────────────────────┘
```

## arrayMap(func, arr1, ...)

Returns an array obtained from the original arrays by application of `func(arr1[i], ..., arrN[i])` for each element. Arrays `arr1` ... `arrN` must have the same number of elements.

Examples:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

The following example shows how to create a tuple of elements from different arrays:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

Note that the `arrayMap` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayFilter(func, arr1, ...)

Returns an array containing only the elements in `arr1` for which `func(arr1[i], ..., arrN[i])` returns something other than 0.

Examples:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

``` text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

``` text
┌─res─┐
│ [2] │
└─────┘
```

Note that the `arrayFilter` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayFill(func, arr1, ...)

Scan through `arr1` from the first element to the last element and replace `arr1[i]` by `arr1[i - 1]` if `func(arr1[i], ..., arrN[i])` returns 0. The first element of `arr1` will not be replaced.

Examples:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

Note that the `arrayFill` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayReverseFill(func, arr1, ...)

Scan through `arr1` from the last element to the first element and replace `arr1[i]` by `arr1[i + 1]` if `func(arr1[i], ..., arrN[i])` returns 0. The last element of `arr1` will not be replaced.

Examples:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

Note that the `arrayReverseFill` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arraySplit(func, arr1, ...)

Split `arr1` into multiple arrays. When `func(arr1[i], ..., arrN[i])` returns something other than 0, the array will be split on the left hand side of the element. The array will not be split before the first element.

Examples:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

Note that the `arraySplit` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayReverseSplit(func, arr1, ...)

Split `arr1` into multiple arrays. When `func(arr1[i], ..., arrN[i])` returns something other than 0, the array will be split on the right hand side of the element. The array will not be split after the last element.

Examples:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

Note that the `arrayReverseSplit` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayExists(\[func,\] arr1, ...)

Returns 1 if there is at least one element in `arr` for which `func(arr1[i], ..., arrN[i])` returns something other than 0. Otherwise, it returns 0.

Note that the `arrayExists` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

## arrayAll(\[func,\] arr1, ...)

Returns 1 if `func(arr1[i], ..., arrN[i])` returns something other than 0 for all the elements in arrays. Otherwise, it returns 0.

Note that the `arrayAll` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

## arrayFirst(func, arr1, ...)

Returns the first element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0.

## arrayFirstOrNull

Returns the first element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0, otherwise it returns `NULL`.

**Syntax**

```sql
arrayFirstOrNull(func, arr1, ...)
```

**Parameters**

- `func`: Lambda function. [Lambda function](../functions/#higher-order-functions---operator-and-lambdaparams-expr-function).
- `arr1`: Array to operate on. [Array](../data-types/array.md).

**Returned value**

- The first element in the passed array.
- Otherwise, returns `NULL`

**Implementation details**

Note that the `arrayFirstOrNull` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

**Example**

Query:

```sql
SELECT arrayFirstOrNull(x -> x >= 2, [1, 2, 3]);
```

Result:

```response
2
```

Query:

```sql
SELECT arrayFirstOrNull(x -> x >= 2, emptyArrayUInt8());
```

Result:

```response
\N
```

Query:

```sql
SELECT arrayLastOrNull((x,f) -> f, [1,2,3,NULL], [0,1,0,1]);
```

Result:

```response
\N
```

## arrayLast(func, arr1, ...)

Returns the last element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0.

Note that the `arrayLast` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayLastOrNull

Returns the last element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0, otherwise returns `NULL`.

**Syntax**

```sql
arrayLastOrNull(func, arr1, ...)
```

**Parameters**

- `func`: Lambda function. [Lambda function](../functions/#higher-order-functions---operator-and-lambdaparams-expr-function).
- `arr1`: Array to operate on. [Array](../data-types/array.md).

**Returned value**

- The last element in the passed array.
- Otherwise, returns `NULL`

**Implementation details**

Note that the `arrayLastOrNull` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

**Example**

Query:

```sql
SELECT arrayLastOrNull(x -> x >= 2, [1, 2, 3]);
```

Result:

```response
3
```

Query:

```sql
SELECT arrayLastOrNull(x -> x >= 2, emptyArrayUInt8());
```

Result:

```response
\N
```

## arrayFirstIndex(func, arr1, ...)

Returns the index of the first element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0.

Note that the `arrayFirstIndex` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayLastIndex(func, arr1, ...)

Returns the index of the last element in the `arr1` array for which `func(arr1[i], ..., arrN[i])` returns something other than 0.

Note that the `arrayLastIndex` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You must pass a lambda function to it as the first argument, and it can’t be omitted.

## arrayMin

Returns the minimum of elements in the source array.

If the `func` function is specified, returns the mininum of elements converted by this function.

Note that the `arrayMin` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayMin([func,] arr)
```

**Arguments**

- `func` — Function. [Expression](../data-types/special-data-types/expression.md).
- `arr` — Array. [Array](../data-types/array.md).

**Returned value**

- The minimum of function values (or the array minimum).

:::note
If `func` is specified, then the return type matches the return value type of `func`, otherwise it matches the type of the array elements.
:::

**Examples**

Query:

```sql
SELECT arrayMin([1, 2, 4]) AS res;
```

Result:

```text
┌─res─┐
│   1 │
└─────┘
```

Query:

```sql
SELECT arrayMin(x -> (-x), [1, 2, 4]) AS res;
```

Result:

```text
┌─res─┐
│  -4 │
└─────┘
```

## arrayMax

Returns the maximum of elements in the source array.

If the `func` function is specified, returns the maximum of elements converted by this function.

Note that the `arrayMax` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayMax([func,] arr)
```

**Arguments**

- `func` — Function. [Expression](../data-types/special-data-types/expression.md).
- `arr` — Array. [Array](../data-types/array.md).

**Returned value**

- The maximum of function values (or the array maximum).

:::note
if `func` is specified then the return type matches the return value type of `func`, otherwise it matches the type of the array elements.
:::

**Examples**

Query:

```sql
SELECT arrayMax([1, 2, 4]) AS res;
```

Result:

```text
┌─res─┐
│   4 │
└─────┘
```

Query:

```sql
SELECT arrayMax(x -> (-x), [1, 2, 4]) AS res;
```

Result:

```text
┌─res─┐
│  -1 │
└─────┘
```

## arraySum

Returns the sum of elements in the source array.

If the `func` function is specified, returns the sum of elements converted by this function.

Note that the `arraySum` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arraySum([func,] arr)
```

**Arguments**

- `func` — Function. [Expression](../data-types/special-data-types/expression.md).
- `arr` — Array. [Array](../data-types/array.md).

**Returned value**

- The sum of the function values (or the array sum).

:::note
Return type:

- For decimal numbers in the source array (or for converted values, if `func` is specified) — [Decimal128](../data-types/decimal.md).
- For floating point numbers — [Float64](../data-types/float.md).
- For numeric unsigned — [UInt64](../data-types/int-uint.md). 
- For numeric signed — [Int64](../data-types/int-uint.md).
:::

**Examples**

Query:

```sql
SELECT arraySum([2, 3]) AS res;
```

Result:

```text
┌─res─┐
│   5 │
└─────┘
```

Query:

```sql
SELECT arraySum(x -> x*x, [2, 3]) AS res;
```

Result:

```text
┌─res─┐
│  13 │
└─────┘
```

## arrayAvg

Returns the average of elements in the source array.

If the `func` function is specified, returns the average of elements converted by this function.

Note that the `arrayAvg` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Syntax**

```sql
arrayAvg([func,] arr)
```

**Arguments**

- `func` — Function. [Expression](../data-types/special-data-types/expression.md).
- `arr` — Array. [Array](../data-types/array.md).

**Returned value**

- The average of function values (or the array average). [Float64](../data-types/float.md).

**Examples**

Query:

```sql
SELECT arrayAvg([1, 2, 4]) AS res;
```

Result:

```text
┌────────────────res─┐
│ 2.3333333333333335 │
└────────────────────┘
```

Query:

```sql
SELECT arrayAvg(x -> (x * x), [2, 4]) AS res;
```

Result:

```text
┌─res─┐
│  10 │
└─────┘
```

## arrayCumSum(\[func,\] arr1, ...)

Returns an array of the partial (running) sums of the elements in the source array `arr1`. If `func` is specified, then the sum is computed from applying `func` to `arr1`, `arr2`, ..., `arrN`, i.e. `func(arr1[i], ..., arrN[i])`.

**Syntax**

``` sql
arrayCumSum(arr)
```

**Arguments**

- `arr` — [Array](../data-types/array.md) of numeric values.

**Returned value**

- Returns an array of the partial sums of the elements in the source array. [UInt\*](https://clickhouse.com/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.com/docs/en/data_types/int_uint/#int-ranges), [Float\*](https://clickhouse.com/docs/en/data_types/float/).

Example:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

Note that the `arrayCumSum` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

## arrayCumSumNonNegative(\[func,\] arr1, ...)

Same as `arrayCumSum`, returns an array of the partial (running) sums of the elements in the source array. If `func` is specified, then the sum is computed from applying `func` to `arr1`, `arr2`, ..., `arrN`, i.e. `func(arr1[i], ..., arrN[i])`. Unlike `arrayCumSum`, if the current running sum is smaller than `0`, it is replaced by `0`.

**Syntax**

``` sql
arrayCumSumNonNegative(arr)
```

**Arguments**

- `arr` — [Array](../data-types/array.md) of numeric values.

**Returned value**

- Returns an array of non-negative partial sums of elements in the source array. [UInt\*](https://clickhouse.com/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.com/docs/en/data_types/int_uint/#int-ranges), [Float\*](https://clickhouse.com/docs/en/data_types/float/).

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

Note that the `arraySumNonNegative` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

## arrayProduct

Multiplies elements of an [array](../data-types/array.md).

**Syntax**

``` sql
arrayProduct(arr)
```

**Arguments**

- `arr` — [Array](../data-types/array.md) of numeric values.

**Returned value**

- A product of array's elements. [Float64](../data-types/float.md).

**Examples**

Query:

``` sql
SELECT arrayProduct([1,2,3,4,5,6]) as res;
```

Result:

``` text
┌─res───┐
│ 720   │
└───────┘
```

Query:

``` sql
SELECT arrayProduct([toDecimal64(1,8), toDecimal64(2,8), toDecimal64(3,8)]) as res, toTypeName(res);
```

Return value type is always [Float64](../data-types/float.md). Result:

``` text
┌─res─┬─toTypeName(arrayProduct(array(toDecimal64(1, 8), toDecimal64(2, 8), toDecimal64(3, 8))))─┐
│ 6   │ Float64                                                                                  │
└─────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

## arrayRotateLeft

Rotates an [array](../data-types/array.md) to the left by the specified number of elements.
If the number of elements is negative, the array is rotated to the right.

**Syntax**

``` sql
arrayRotateLeft(arr, n)
```

**Arguments**

- `arr` — [Array](../data-types/array.md).
- `n` — Number of elements to rotate.

**Returned value**

- An array rotated to the left by the specified number of elements. [Array](../data-types/array.md).

**Examples**

Query:

``` sql
SELECT arrayRotateLeft([1,2,3,4,5,6], 2) as res;
```

Result:

``` text
┌─res───────────┐
│ [3,4,5,6,1,2] │
└───────────────┘
```

Query:

``` sql
SELECT arrayRotateLeft([1,2,3,4,5,6], -2) as res;
```

Result:

``` text
┌─res───────────┐
│ [5,6,1,2,3,4] │
└───────────────┘
```

Query:

``` sql
SELECT arrayRotateLeft(['a','b','c','d','e'], 3) as res;
```

Result:

``` text
┌─res───────────────────┐
│ ['d','e','a','b','c'] │
└───────────────────────┘
```

## arrayRotateRight

Rotates an [array](../data-types/array.md) to the right by the specified number of elements.
If the number of elements is negative, the array is rotated to the left.

**Syntax**

``` sql
arrayRotateRight(arr, n)
```

**Arguments**

- `arr` — [Array](../data-types/array.md).
- `n` — Number of elements to rotate.

**Returned value**

- An array rotated to the right by the specified number of elements. [Array](../data-types/array.md).

**Examples**

Query:

``` sql
SELECT arrayRotateRight([1,2,3,4,5,6], 2) as res;
```

Result:

``` text
┌─res───────────┐
│ [5,6,1,2,3,4] │
└───────────────┘
```

Query:

``` sql
SELECT arrayRotateRight([1,2,3,4,5,6], -2) as res;
```

Result:

``` text
┌─res───────────┐
│ [3,4,5,6,1,2] │
└───────────────┘
```

Query:

``` sql
SELECT arrayRotateRight(['a','b','c','d','e'], 3) as res;
```

Result:

``` text
┌─res───────────────────┐
│ ['c','d','e','a','b'] │
└───────────────────────┘
```

## arrayShiftLeft

Shifts an [array](../data-types/array.md) to the left by the specified number of elements.
New elements are filled with the provided argument or the default value of the array element type.
If the number of elements is negative, the array is shifted to the right.

**Syntax**

``` sql
arrayShiftLeft(arr, n[, default])
```

**Arguments**

- `arr` — [Array](../data-types/array.md).
- `n` — Number of elements to shift.
- `default` — Optional. Default value for new elements.

**Returned value**

- An array shifted to the left by the specified number of elements. [Array](../data-types/array.md).

**Examples**

Query:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], 2) as res;
```

Result:

``` text
┌─res───────────┐
│ [3,4,5,6,0,0] │
└───────────────┘
```

Query:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], -2) as res;
```

Result:

``` text
┌─res───────────┐
│ [0,0,1,2,3,4] │
└───────────────┘
```

Query:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], 2, 42) as res;
```

Result:

``` text
┌─res─────────────┐
│ [3,4,5,6,42,42] │
└─────────────────┘
```

Query:

``` sql
SELECT arrayShiftLeft(['a','b','c','d','e','f'], 3, 'foo') as res;
```

Result:

``` text
┌─res─────────────────────────────┐
│ ['d','e','f','foo','foo','foo'] │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6] :: Array(UInt16), 2, 4242) as res;
```

Result:

``` text
┌─res─────────────────┐
│ [3,4,5,6,4242,4242] │
└─────────────────────┘
```

## arrayShiftRight

Shifts an [array](../data-types/array.md) to the right by the specified number of elements.
New elements are filled with the provided argument or the default value of the array element type.
If the number of elements is negative, the array is shifted to the left.

**Syntax**

``` sql
arrayShiftRight(arr, n[, default])
```

**Arguments**

- `arr` — [Array](../data-types/array.md).
- `n` — Number of elements to shift.
- `default` — Optional. Default value for new elements.

**Returned value**

- An array shifted to the right by the specified number of elements. [Array](../data-types/array.md).

**Examples**

Query:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], 2) as res;
```

Result:

``` text
┌─res───────────┐
│ [0,0,1,2,3,4] │
└───────────────┘
```

Query:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], -2) as res;
```

Result:

``` text
┌─res───────────┐
│ [3,4,5,6,0,0] │
└───────────────┘
```

Query:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], 2, 42) as res;
```

Result:

``` text
┌─res─────────────┐
│ [42,42,1,2,3,4] │
└─────────────────┘
```

Query:

``` sql
SELECT arrayShiftRight(['a','b','c','d','e','f'], 3, 'foo') as res;
```

Result:

``` text
┌─res─────────────────────────────┐
│ ['foo','foo','foo','a','b','c'] │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6] :: Array(UInt16), 2, 4242) as res;
```

Result:

``` text
┌─res─────────────────┐
│ [4242,4242,1,2,3,4] │
└─────────────────────┘
```


## arrayRandomSample

Function `arrayRandomSample` returns a subset with `samples`-many random elements of an input array. If `samples` exceeds the size of the input array, the sample size is limited to the size of the array, i.e. all array elements are returned but their order is not guaranteed. The function can handle both flat arrays and nested arrays.

**Syntax**

```sql
arrayRandomSample(arr, samples)
```

**Arguments**

- `arr` — The input array from which to sample elements. ([Array(T)](../data-types/array.md))
- `samples` — The number of elements to include in the random sample ([UInt*](../data-types/int-uint.md))

**Returned Value**

- An array containing a random sample of elements from the input array. [Array](../data-types/array.md).

**Examples**

Query:

```sql
SELECT arrayRandomSample(['apple', 'banana', 'cherry', 'date'], 2) as res;
```

Result:

```
┌─res────────────────┐
│ ['cherry','apple'] │
└────────────────────┘
```

Query:

```sql
SELECT arrayRandomSample([[1, 2], [3, 4], [5, 6]], 2) as res;
```

Result:

```
┌─res───────────┐
│ [[3,4],[5,6]] │
└───────────────┘
```

Query:

```sql
SELECT arrayRandomSample([1, 2, 3], 5) as res;
```

Result:

```
┌─res─────┐
│ [3,1,2] │
└─────────┘
```

## Distance functions

All supported functions are described in [distance functions documentation](../../sql-reference/functions/distance-functions.md).