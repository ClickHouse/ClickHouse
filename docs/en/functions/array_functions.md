# Functions for working with arrays

## empty

Returns 1 for an empty array, or 0 for a non-empty array.
The result type is UInt8.
The function also works for strings.

## notEmpty

Returns 0 for an empty array, or 1 for a non-empty array.
The result type is UInt8.
The function also works for strings.

## length

Returns the number of items in the array.
The result type is UInt64.
The function also works for strings.

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64

## emptyArrayFloat32, emptyArrayFloat64

## emptyArrayDate, emptyArrayDateTime

## emptyArrayString

Accepts zero arguments and returns an empty array of the appropriate type.

## emptyArrayToSingle

Accepts an empty array and returns a one-element array that is equal to the default value.

## range(N)

Returns an array of numbers from 0 to N-1.
Just in case, an exception is thrown if arrays with a total length of more than 100,000,000 elements are created in a data block.

## array(x1, ...), operator \[x1, ...\]

Creates an array from the function arguments.
The arguments must be constants and have types that have the smallest common type. At least one argument must be passed, because otherwise it isn't clear which type of array to create. That is, you can't use this function to create an empty array (to do that, use the 'emptyArray\*' function described above).
Returns an 'Array(T)' type result, where 'T' is the smallest common type out of the passed arguments.

## arrayConcat

Combines arrays passed as arguments.

```
arrayConcat(arrays)
```

**Arguments**

- `arrays` – Arrays of comma-separated `[values]`.

**Example**

```sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```
```
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), operator arr[n]

Get the element with the index 'n' from the array 'arr'.'n' must be any integer type.
Indexes in an array begin from one.
Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, 'arr\[-1\]' is the last item in the array.

If the index falls outside of the bounds of an array, it returns some default value (0 for numbers, an empty string for strings, etc.).

## has(arr, elem)

Checks whether the 'arr' array has the 'elem' element.
Returns 0 if the the element is not in the array, or 1 if it is.

## indexOf(arr, x)

Returns the index of the 'x' element (starting from 1) if it is in the array, or 0 if it is not.

## countEqual(arr, x)

Returns the number of elements in the array equal to x. Equivalent to arrayCount (elem-&gt;  elem = x, arr).

## arrayEnumerate(arr)

Returns the array \[1, 2, 3, ..., length (arr) \]

This function is normally used with ARRAY JOIN. It allows counting something just once for each array after applying ARRAY JOIN. Example:

```sql
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

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

In this example, Reaches is the number of conversions (the strings received after applying ARRAY JOIN), and Hits is the number of pageviews (strings before ARRAY JOIN). In this particular case, you can get the same result in an easier way:

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

This function can also be used in higher-order functions. For example, you can use it to get array indexes for elements that match a condition.

## arrayEnumerateUniq(arr, ...)

Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.
For example: arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1,  1,  2,  1\].

This function is useful when using ARRAY JOIN and aggregation of array elements.
Example:

```sql
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

```text
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

```sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

```text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

This is necessary when using ARRAY JOIN with a nested data structure and further aggregation across multiple elements in this structure.

## arrayPopBack

Removes the last item from the array.

```
arrayPopBack(array)
```

**Arguments**

- `array` – Array.

**Example**

```sql
SELECT arrayPopBack([1, 2, 3]) AS res
```
```
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront

Removes the first item from the array.

```
arrayPopFront(array)
```

**Arguments**

- `array` – Array.

**Example**

```sql
SELECT arrayPopFront([1, 2, 3]) AS res
```
```
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack

Adds one item to the end of the array.

```
arrayPushBack(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array. For more information about ClickHouse data types, read the section "[Data types](../data_types/index.md#data_types)".

**Example**

```sql
SELECT arrayPushBack(['a'], 'b') AS res
```

```
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront

Adds one element to the beginning of the array.

```
arrayPushFront(array, single_value)
```

**Arguments**

- `array` – Array.
- `single_value` – A single value.  Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` type for the data type of the array.  For more information about ClickHouse data types, read the section "[Data types](../data_types/index.md#data_types)".

**Example**

```sql
SELECT arrayPushBack(['b'], 'a') AS res
```
```
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arraySlice

Returns a slice of the array.

```
arraySlice(array, offset[, length])
```

**Arguments**

- `array` –  Array of data.
- `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
- `length` - The length of the required slice. If you specify a negative value, the function returns an open slice `[offset, array_length - length)`. If you omit the value, the function returns the slice `[offset, the_end_of_array]`.

**Example**

```sql
SELECT arraySlice([1, 2, 3, 4, 5], 2, 3) AS res
```
```
┌─res─────┐
│ [2,3,4] │
└─────────┘
```

## arrayUniq(arr, ...)

If one argument is passed, it counts the number of different elements in the array.
If multiple arguments are passed, it counts the number of different tuples of elements at corresponding positions in multiple arrays.

If you want to get a list of unique items in an array, you can use arrayReduce('groupUniqArray', arr).

## arrayJoin(arr)

A special function. See the section ["ArrayJoin function"](array_join.md#functions_arrayjoin).

