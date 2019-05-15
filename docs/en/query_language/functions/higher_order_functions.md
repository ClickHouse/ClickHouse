# Higher-order functions

## `->` operator, lambda(params, expr) function

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Examples: `x -> 2 * x, str -> str != Referer.`

Higher-order functions can only accept lambda functions as their functional argument.

A lambda function that accepts multiple arguments can be passed to a higher-order function. In this case, the higher-order function is passed several arrays of identical length that these arguments will correspond to.

For all functions other than 'arrayMap' and 'arrayFilter', the first argument (the lambda function) can be omitted. In this case, identical mapping is assumed.

### arrayMap(func, arr1, ...)

Returns an array obtained from the original application of the 'func' function to each element in the 'arr' array.

### arrayFilter(func, arr1, ...)

Returns an array containing only the elements in 'arr1' for which 'func' returns something other than 0.

Examples:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

```
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

```
┌─res─┐
│ [2] │
└─────┘
```

### arrayCount(\[func,\] arr1, ...)

Returns the number of elements in the arr array for which func returns something other than 0. If 'func' is not specified, it returns the number of non-zero elements in the array.

### arrayExists(\[func,\] arr1, ...)

Returns 1 if there is at least one element in 'arr' for which 'func' returns something other than 0. Otherwise, it returns 0.

### arrayAll(\[func,\] arr1, ...)

Returns 1 if 'func' returns something other than 0 for all the elements in 'arr'. Otherwise, it returns 0.

### arraySum(\[func,\] arr1, ...)

Returns the sum of the 'func' values. If the function is omitted, it just returns the sum of the array elements.

### arrayFirst(func, arr1, ...)

Returns the first element in the 'arr1' array for which 'func' returns something other than 0.

### arrayFirstIndex(func, arr1, ...)

Returns the index of the first element in the 'arr1' array for which 'func' returns something other than 0.

### arrayCumSum(\[func,\] arr1, ...)

Returns an array of partial sums of elements in the source array (a running sum). If the `func` function is specified, then the values of the array elements are converted by this function before summing.

Example:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

```
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative(arr)

Same as arrayCumSum, returns an array of partial sums of elements in the source array (a running sum). Different arrayCumSum, when then returned value contains a value less than zero, the value is replace with zero and the subsequent calculation is performed with zero parameters. For example:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

```
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, ...)

Returns an array as result of sorting the elements of `arr1` in ascending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays)  

The [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used to improve sorting efficiency.

Example:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

```
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Note that NULLs and NaNs go last (NaNs go before NULLs). For example:
 
``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, 4, NULL])
```
```
┌─arraySort([1, nan, 2, NULL, 3, nan, 4, NULL])─┐
│ [1,2,3,4,nan,nan,NULL,NULL]                   │
└───────────────────────────────────────────────┘
```

### arrayReverseSort(\[func,\] arr1, ...)

Returns an array as result of sorting the elements of `arr1` in descending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays)  

Note that NULLs and NaNs go last (NaNs go before NULLs). For example:
 
``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])
```
```
┌─arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])─┐
│ [4,3,2,1,nan,nan,NULL,NULL]                          │
└──────────────────────────────────────────────────────┘
```




[Original article](https://clickhouse.yandex/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
