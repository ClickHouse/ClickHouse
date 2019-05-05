# 高阶函数

## `->` 运算符, lambda(params, expr) 函数

允许描述lambda函数以传递给更高阶函数。 箭头的左侧有一个形式参数，它是任何ID或多个形式参数 - 元组中的任何ID。 箭头的右侧有一个表达式，可以使用这些形式参数以及任何表格列。
Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Examples: `x -> 2 * x, str -> str != Referer.`

高阶函数只能接受lambda函数作为其函数参数。
Higher-order functions can only accept lambda functions as their functional argument.

接受多个参数的lambda函数可以传递给更高阶的函数。 在这种情况下，高阶函数被传递几个长度相同的数组，这些数组将对应于这些数组。
A lambda function that accepts multiple arguments can be passed to a higher-order function. In this case, the higher-order function is passed several arrays of identical length that these arguments will correspond to.

对于除'arrayMap'和'arrayFilter'以外的所有函数，可以省略第一个参数（lambda函数）。 在这种情况下，假设相同的映射。
For all functions other than 'arrayMap' and 'arrayFilter', the first argument (the lambda function) can be omitted. In this case, identical mapping is assumed.

### arrayMap(func, arr1, ...)

将从'func'函数的原始应用程序获得的数组返回到'arr'数组中的每个元素。
Returns an array obtained from the original application of the 'func' function to each element in the 'arr' array.

### arrayFilter(func, arr1, ...)

返回一个只包含'arr1'中元素的数组，'func'返回0以外的元素。
Returns an array containing only the elements in 'arr1' for which 'func' returns something other than 0.

示例:

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

返回arr数组中func返回0以外的元素的数量。如果未指定'func'，则返回数组中非零元素的数量。
Returns the number of elements in the arr array for which func returns something other than 0. If 'func' is not specified, it returns the number of non-zero elements in the array.

### arrayExists(\[func,\] arr1, ...)

如果'arr'中至少有一个元素为'func'返回0以外的值，则返回1.否则返回0。
Returns 1 if there is at least one element in 'arr' for which 'func' returns something other than 0. Otherwise, it returns 0.

### arrayAll(\[func,\] arr1, ...)

如果'func'为'arr'中的所有元素返回0以外的值，则返回1。 否则，它返回0。
Returns 1 if 'func' returns something other than 0 for all the elements in 'arr'. Otherwise, it returns 0.

### arraySum(\[func,\] arr1, ...)

返回'func'值的总和。 如果省略该函数，它只返回数组元素的总和。
Returns the sum of the 'func' values. If the function is omitted, it just returns the sum of the array elements.

### arrayFirst(func, arr1, ...)

返回'arr1'数组中的第一个元素，'func'返回0以外的值。
Returns the first element in the 'arr1' array for which 'func' returns something other than 0.

### arrayFirstIndex(func, arr1, ...)

返回'arr1'数组中第一个元素的索引，'func'返回0以外的值。
Returns the index of the first element in the 'arr1' array for which 'func' returns something other than 0.

### arrayCumSum(\[func,\] arr1, ...)

返回源数组中元素的部分和的数组（运行总和）。 如果指定了`func`函数，那么在求和之前，该函数将转换数组元素的值。
Returns an array of partial sums of elements in the source array (a running sum). If the `func` function is specified, then the values of the array elements are converted by this function before summing.

示例:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

```
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative(arr)

与arrayCumSum相同，返回源数组中元素的部分和的数组（运行总和）。 不同的arrayCumSum，当返回值包含小于零的值时，该值替换为零，后续计算使用零参数执行。 例如：
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

返回一个数组，作为以递增顺序排列`arr1`元素的结果。 如果指定了`func`函数，则排序顺序由应用于数组（数组）元素的函数`func`的结果确定
Returns an array as result of sorting the elements of `arr1` in ascending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays)  

[Schwartzian变换](https://en.wikipedia.org/wiki/Schwartzian_transform)用于提高分类效率。
The [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used to improve sorting efficiency.

示例:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

```
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

请注意，NULL和NaN最后一次（NaN在NULL之前）。 例如：
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

返回一个数组，作为按顺序排序`arr1`元素的结果。 如果指定了`func`函数，则排序顺序由应用于数组（数组）元素的函数`func`的结果确定
Returns an array as result of sorting the elements of `arr1` in descending order. If the `func` function is specified, sorting order is determined by the result of the function `func` applied to the elements of array (arrays)  

请注意，NULL和NaN最后一次（NaN在NULL之前）。例如：
Note that NULLs and NaNs go last (NaNs go before NULLs). For example:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])
```
```
┌─arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])─┐
│ [4,3,2,1,nan,nan,NULL,NULL]                          │
└──────────────────────────────────────────────────────┘
```




[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
