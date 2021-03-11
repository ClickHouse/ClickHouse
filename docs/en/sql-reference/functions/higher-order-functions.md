---
toc_priority: 57
toc_title: Higher-Order
---

# Higher-order Functions {#higher-order-functions}

## `->` operator, lambda(params, expr) function {#operator-lambdaparams-expr-function}

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Examples: `x -> 2 * x, str -> str != Referer.`

Higher-order functions can only accept lambda functions as their functional argument.

A lambda function that accepts multiple arguments can be passed to a higher-order function. In this case, the higher-order function is passed several arrays of identical length that these arguments will correspond to.

For some functions, such as [arrayCount](#higher_order_functions-array-count) or [arraySum](#higher_order_functions-array-count), the first argument (the lambda function) can be omitted. In this case, identical mapping is assumed.

A lambda function can’t be omitted for the following functions:

-   [arrayMap](#higher_order_functions-array-map)
-   [arrayFilter](#higher_order_functions-array-filter)
-   [arrayFill](#higher_order_functions-array-fill)
-   [arrayReverseFill](#higher_order_functions-array-reverse-fill)
-   [arraySplit](#higher_order_functions-array-split)
-   [arrayReverseSplit](#higher_order_functions-array-reverse-split)
-   [arrayFirst](#higher_order_functions-array-first)
-   [arrayFirstIndex](#higher_order_functions-array-first-index)

### arrayMap(func, arr1, …) {#higher_order_functions-array-map}

Returns an array obtained from the original application of the `func` function to each element in the `arr` array.

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

Note that the first argument (lambda function) can’t be omitted in the `arrayMap` function.

### arrayFilter(func, arr1, …) {#higher_order_functions-array-filter}

Returns an array containing only the elements in `arr1` for which `func` returns something other than 0.

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

Note that the first argument (lambda function) can’t be omitted in the `arrayFilter` function.

### arrayFill(func, arr1, …) {#higher_order_functions-array-fill}

Scan through `arr1` from the first element to the last element and replace `arr1[i]` by `arr1[i - 1]` if `func` returns 0. The first element of `arr1` will not be replaced.

Examples:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

Note that the first argument (lambda function) can’t be omitted in the `arrayFill` function.

### arrayReverseFill(func, arr1, …) {#higher_order_functions-array-reverse-fill}

Scan through `arr1` from the last element to the first element and replace `arr1[i]` by `arr1[i + 1]` if `func` returns 0. The last element of `arr1` will not be replaced.

Examples:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

Note that the first argument (lambda function) can’t be omitted in the `arrayReverseFill` function.

### arraySplit(func, arr1, …) {#higher_order_functions-array-split}

Split `arr1` into multiple arrays. When `func` returns something other than 0, the array will be split on the left hand side of the element. The array will not be split before the first element.

Examples:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

Note that the first argument (lambda function) can’t be omitted in the `arraySplit` function.

### arrayReverseSplit(func, arr1, …) {#higher_order_functions-array-reverse-split}

Split `arr1` into multiple arrays. When `func` returns something other than 0, the array will be split on the right hand side of the element. The array will not be split after the last element.

Examples:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

Note that the first argument (lambda function) can’t be omitted in the `arraySplit` function.

### arrayCount(\[func,\] arr1, …) {#higher_order_functions-array-count}

Returns the number of elements in the arr array for which func returns something other than 0. If ‘func’ is not specified, it returns the number of non-zero elements in the array.

### arrayExists(\[func,\] arr1, …) {#arrayexistsfunc-arr1}

Returns 1 if there is at least one element in ‘arr’ for which ‘func’ returns something other than 0. Otherwise, it returns 0.

### arrayAll(\[func,\] arr1, …) {#arrayallfunc-arr1}

Returns 1 if ‘func’ returns something other than 0 for all the elements in ‘arr’. Otherwise, it returns 0.

### arraySum(\[func,\] arr1, …) {#higher-order-functions-array-sum}

Returns the sum of the ‘func’ values. If the function is omitted, it just returns the sum of the array elements.

### arrayFirst(func, arr1, …) {#higher_order_functions-array-first}

Returns the first element in the ‘arr1’ array for which ‘func’ returns something other than 0.

Note that the first argument (lambda function) can’t be omitted in the `arrayFirst` function.

### arrayFirstIndex(func, arr1, …) {#higher_order_functions-array-first-index}

Returns the index of the first element in the ‘arr1’ array for which ‘func’ returns something other than 0.

Note that the first argument (lambda function) can’t be omitted in the `arrayFirstIndex` function.

### arrayCumSum(\[func,\] arr1, …) {#arraycumsumfunc-arr1}

Returns an array of partial sums of elements in the source array (a running sum). If the `func` function is specified, then the values of the array elements are converted by this function before summing.

Example:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative(arr) {#arraycumsumnonnegativearr}

Same as `arrayCumSum`, returns an array of partial sums of elements in the source array (a running sum). Different `arrayCumSum`, when then returned value contains a value less than zero, the value is replace with zero and the subsequent calculation is performed with zero parameters. For example:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, …) {#arraysortfunc-arr1}

Returns an array as result of sorting the elements of `arr1` in ascending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays)

The [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used to improve sorting efficiency.

Example:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

For more information about the `arraySort` method, see the [Functions for Working With Arrays](../../sql-reference/functions/array-functions.md#array_functions-sort) section.

### arrayReverseSort(\[func,\] arr1, …) {#arrayreversesortfunc-arr1}

Returns an array as result of sorting the elements of `arr1` in descending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays).

Example:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

For more information about the `arrayReverseSort` method, see the [Functions for Working With Arrays](../../sql-reference/functions/array-functions.md#array_functions-reverse-sort) section.

[Original article](https://clickhouse.tech/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
