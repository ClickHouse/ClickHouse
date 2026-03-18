---
slug: /zh/sql-reference/functions/higher-order-functions
---
# 高阶函数 {#gao-jie-han-shu}

## `->` 运算符, lambda(params, expr) 函数 {#yun-suan-fu-lambdaparams-expr-han-shu}

用于描述一个lambda函数用来传递给其他高阶函数。箭头的左侧有一个形式参数，它可以是一个标识符或多个标识符所组成的元祖。箭头的右侧是一个表达式，在这个表达式中可以使用形式参数列表中的任何一个标识符或表的任何一个列名。

示例: `x -> 2 * x, str -> str != Referer.`

高阶函数只能接受lambda函数作为其参数。

高阶函数可以接受多个参数的lambda函数作为其参数，在这种情况下，高阶函数需要同时传递几个长度相等的数组，这些数组将被传递给lambda参数。

除了’arrayMap’和’arrayFilter’以外的所有其他函数，都可以省略第一个参数（lambda函数）。在这种情况下，默认返回数组元素本身。

### arrayMap(func, arr1, ...) {#higher_order_functions-array-map}

将arr
将从’func’函数的原始应用程序获得的数组返回到’arr’数组中的每个元素。
返回从原始应用程序获得的数组 ‘func’ 函数中的每个元素 ‘arr’ 阵列。

### arrayFilter(func, arr1, ...) {#arrayfilterfunc-arr1}

返回一个仅包含以下元素的数组 ‘arr1’ 对于哪个 ‘func’ 返回0以外的内容。

示例:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

    ┌─res───────────┐
    │ ['abc World'] │
    └───────────────┘

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

    ┌─res─┐
    │ [2] │
    └─────┘

### arrayCount(\[func,\] arr1, ...) {#arraycountfunc-arr1}

返回数组arr中非零元素的数量，如果指定了’func’，则通过’func’的返回值确定元素是否为非零元素。

### arrayExists(\[func,\] arr1, ...) {#arrayexistsfunc-arr1}

返回数组’arr’中是否存在非零元素，如果指定了’func’，则使用’func’的返回值确定元素是否为非零元素。

### arrayAll(\[func,\] arr1, ...) {#arrayallfunc-arr1}

返回数组’arr’中是否存在为零的元素，如果指定了’func’，则使用’func’的返回值确定元素是否为零元素。

### arraySum(\[func,\] arr1, ...) {#arraysumfunc-arr1}

计算arr数组的总和，如果指定了’func’，则通过’func’的返回值计算数组的总和。

### arrayFirst(func, arr1, ...) {#arrayfirstfunc-arr1}

返回数组中第一个匹配的元素，函数使用’func’匹配所有元素，直到找到第一个匹配的元素。

### arrayFirstIndex(func, arr1, ...) {#arrayfirstindexfunc-arr1}

返回数组中第一个匹配的元素的下标索引，函数使用’func’匹配所有元素，直到找到第一个匹配的元素。

### arrayCumSum(\[func,\] arr1, ...) {#arraycumsumfunc-arr1}

返回源数组部分数据的总和，如果指定了`func`函数，则使用`func`的返回值计算总和。

示例:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

    ┌─res──────────┐
    │ [1, 2, 3, 4] │
    └──────────────┘

### arrayCumSumNonNegative(arr) {#arraycumsumnonnegativearr}

与arrayCumSum相同，返回源数组部分数据的总和。不同于arrayCumSum，当返回值包含小于零的值时，该值替换为零，后续计算使用零继续计算。例如：

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

    ┌─res───────┐
    │ [1,2,0,1] │
    └───────────┘

### arraySort(\[func,\] arr1, ...) {#arraysortfunc-arr1}

返回升序排序`arr1`的结果。如果指定了`func`函数，则排序顺序由`func`的结果决定。

[Schwartzian变换](https://en.wikipedia.org/wiki/Schwartzian_transform)用于提高排序效率。

示例:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

    ┌─res────────────────┐
    │ ['world', 'hello'] │
    └────────────────────┘

请注意，NULL和NaN在最后（NaN在NULL之前）。例如：

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, 4, NULL])
```

    ┌─arraySort([1, nan, 2, NULL, 3, nan, 4, NULL])─┐
    │ [1,2,3,4,nan,nan,NULL,NULL]                   │
    └───────────────────────────────────────────────┘

### arrayReverseSort(\[func,\] arr1, ...) {#arrayreversesortfunc-arr1}

返回降序排序`arr1`的结果。如果指定了`func`函数，则排序顺序由`func`的结果决定。

请注意，NULL和NaN在最后（NaN在NULL之前）。例如：

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])
```

    ┌─arrayReverseSort([1, nan, 2, NULL, 3, nan, 4, NULL])─┐
    │ [4,3,2,1,nan,nan,NULL,NULL]                          │
    └──────────────────────────────────────────────────────┘
