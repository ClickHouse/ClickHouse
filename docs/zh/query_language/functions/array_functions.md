# 数组函数

## empty

对于空数组返回1，对于非空数组返回0。
结果类型是UInt8。
该函数也适用于字符串。

## notEmpty

对于空数组返回0，对于非空数组返回1。
结果类型是UInt8。
该函数也适用于字符串。

## length {#array_functions-length}

返回数组中的元素个数。
结果类型是UInt64。
该函数也适用于字符串。

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64

## emptyArrayFloat32, emptyArrayFloat64

## emptyArrayDate, emptyArrayDateTime

## emptyArrayString

不接受任何参数并返回适当类型的空数组。

## emptyArrayToSingle

接受一个空数组并返回一个仅包含一个默认值元素的数组。

## range(N)

返回从0到N-1的数字数组。
以防万一，如果在数据块中创建总长度超过100,000,000个元素的数组，则抛出异常。

## array(x1, ...), operator \[x1, ...\]

使用函数的参数作为数组元素创建一个数组。
参数必须是常量，并且具有最小公共类型的类型。必须至少传递一个参数，否则将不清楚要创建哪种类型的数组。也就是说，你不能使用这个函数来创建一个空数组（为此，使用上面描述的'emptyArray \ *'函数）。
返回'Array（T）'类型的结果，其中'T'是传递的参数中最小的公共类型。

## arrayConcat

合并参数中传递的所有数组。

```
arrayConcat(arrays)
```

**参数**

- `arrays` – 任意数量的[Array](../../data_types/array.md)类型的参数.
**示例**

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

```
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), operator arr[n]

从数组`arr`中获取索引为“n”的元素。 `n`必须是任何整数类型。
数组中的索引从一开始。
支持负索引。在这种情况下，它选择从末尾开始编号的相应元素。例如，`arr [-1]`是数组中的最后一项。

如果索引超出数组的边界，则返回默认值（数字为0，字符串为空字符串等）。

## has(arr, elem)

检查'arr'数组是否具有'elem'元素。
如果元素不在数组中，则返回0;如果在，则返回1。

`NULL` 值的处理。

```
SELECT has([1, 2, NULL], NULL)

┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## hasAll

检查一个数组是否是另一个数组的子集。

```
hasAll(set, subset)
```

**参数**

- `set` – 具有一组元素的任何类型的数组。
- `subset` – 任何类型的数组，其元素应该被测试为`set`的子集。

**返回值**

- `1`， 如果`set`包含`subset`中的所有元素。
- `0`， 否则。

**特殊的定义**

- 空数组是任何数组的子集。
- “Null”作为数组中的元素值进行处理。
- 忽略两个数组中的元素值的顺序。

**示例**

`SELECT hasAll([], [])` returns 1.

`SELECT hasAll([1, Null], [Null])` returns 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` returns 1.

`SELECT hasAll(['a', 'b'], ['a'])` returns 1.

`SELECT hasAll([1], ['a'])` returns 0.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` returns 0.

## hasAny

检查两个数组是否存在交集。

```
hasAny(array1, array2)
```

**Parameters**

- `array1` – 具有一组元素的任何类型的数组。
- `array2` – 具有一组元素的任何类型的数组。

**返回值**

- `1`， 如果`array1`和`array2`存在交集。
- `0`， 否则。

**特殊的定义**

- “Null”作为数组中的元素值进行处理。
- 忽略两个数组中的元素值的顺序。

**示例**

`SELECT hasAny([1], [])` returns `0`.

`SELECT hasAny([Null], [Null, 1])` returns `1`.

`SELECT hasAny([-128, 1., 512], [1])` returns `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` returns `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` returns `1`.

## indexOf(arr, x)

返回数组中第一个‘x’元素的索引（从1开始），如果‘x’元素不存在在数组中，则返回0。

示例:

```
:) SELECT indexOf([1,3,NULL,NULL],NULL)

SELECT indexOf([1, 3, NULL, NULL], NULL)

┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

设置为“NULL”的元素将作为普通的元素值处理。

## countEqual(arr, x)

返回数组中等于x的元素的个数。相当于arrayCount（elem  - > elem = x，arr）。

`NULL`值将作为单独的元素值处理。

示例:

```
SELECT countEqual([1, 2, NULL, NULL], NULL)

┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## arrayEnumerate(arr) {#array_functions-arrayenumerate}

返回 Array \[1, 2, 3, ..., length (arr) \]

此功能通常与ARRAY JOIN一起使用。它允许在应用ARRAY JOIN后为每个数组计算一次。例如：

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

```
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

在此示例中，Reaches是转换次数（应用ARRAY JOIN后接收的字符串），Hits是浏览量（ARRAY JOIN之前的字符串）。在这种特殊情况下，您可以更轻松地获得相同的结果：

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

此功能也可用于高阶函数。例如，您可以使用它来获取与条件匹配的元素的数组索引。

## arrayEnumerateUniq(arr, ...)

返回与源数组大小相同的数组，其中每个元素表示与其下标对应的源数组元素在源数组中出现的次数。
例如：arrayEnumerateUniq（\ [10,20,10,30 \]）= \ [1,1,2,1 \]。

使用ARRAY JOIN和数组元素的聚合时，此函数很有用。

示例:

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

```
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

在此示例中，每个GoalID都计算转换次数（目标嵌套数据结构中的每个元素都是达到的目标，我们称之为转换）和会话数。如果没有ARRAY JOIN，我们会将会话数计为总和（Sign）。但在这种特殊情况下，行乘以嵌套的Goals结构，因此为了在此之后计算每个会话一次，我们将一个条件应用于arrayEnumerateUniq（Goals.ID）函数的值。

arrayEnumerateUniq函数可以使用与参数大小相同的多个数组。在这种情况下，对于所有阵列中相同位置的元素元组，考虑唯一性。

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

```
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

当使用带有嵌套数据结构的ARRAY JOIN并在此结构中跨多个元素进一步聚合时，这是必需的。

## arrayPopBack

从数组中删除最后一项。

```
arrayPopBack(array)
```

**参数**

- `array` – 数组。

**示例**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

```
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront

从数组中删除第一项。

```
arrayPopFront(array)
```

**参数**

- `array` – 数组。

**示例**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

```
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack

添加一个元素到数组的末尾。

```
arrayPushBack(array, single_value)
```

**参数**

- `array` – 数组。
- `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅“[数据类型](../../data_types/index.md#data_types)”。可以是'NULL`。该函数向数组添加一个“NULL”元素，数组元素的类型转换为`Nullable`。

**示例**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

```
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront

将一个元素添加到数组的开头。

```
arrayPushFront(array, single_value)
```

**Parameters**

- `array` – 数组。
- `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅“[数据类型](../../data_types/index.md#data_types)”。可以是'NULL`。该函数向数组添加一个“NULL”元素，数组元素的类型转换为`Nullable`。

**示例**

``` sql
SELECT arrayPushBack(['b'], 'a') AS res
```

```
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize

更改数组的长度。

```
arrayResize(array, size[, extender])
```

**参数:**

- `array` — 数组.
- `size` — 数组所需的长度。
    - 如果`size`小于数组的原始大小，则数组将从右侧截断。
- 如果`size`大于数组的初始大小，则使用`extender`值或数组项的数据类型的默认值将数组扩展到右侧。
- `extender` — 扩展数组的值。可以是'NULL`。

**返回值:**

一个`size`长度的数组。

**调用示例**

```
SELECT arrayResize([1], 3)

┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

```
SELECT arrayResize([1], 3, NULL)

┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice

返回一个子数组，包含从指定位置的指定长度的元素。

```
arraySlice(array, offset[, length])
```

**参数**

- `array` –  数组。
- `offset` – 数组的偏移。正值表示左侧的偏移量，负值表示右侧的缩进值。数组下标从1开始。
- `length` - 子数组的长度。如果指定负值，则该函数返回`[offset，array_length  -  length`。如果省略该值，则该函数返回`[offset，the_end_of_array]`。

**示例**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

```
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

设置为“NULL”的数组元素作为普通的数组元素值处理。

## arraySort(\[func,\] arr, ...) {#array_functions-reverse-sort}

Sorts the elements of the `arr` array in ascending order. If the `func` function is specified, sorting order is determined by the result of the `func` function applied to the elements of the array. If `func` accepts multiple arguments, the `arraySort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arraySort` description.

Example of integer values sorting:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```
```
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

Example of string values sorting:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```
```
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

Consider the following sorting order for the `NULL`, `NaN` and `Inf` values:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```
```
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

- `-Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL`.
- `Inf` values are right before `NaN`.

Note that `arraySort` is a [higher-order function](higher_order_functions.md). You can pass a lambda function to it as the first argument. In this case, sorting order is determined by the result of the lambda function applied to the elements of the array.

Let's consider the following example:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```
```
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, [1 –> -1, 2 –> -2, 3 –> -3]. Since the `arraySort` function sorts the keys in ascending order, the result is [3, 2, 1]. Thus, the `(x) –> -x` lambda function sets the [descending order](#array_functions-reverse-sort) in a sorting.

The lambda function can accept multiple arguments. In this case, you need to pass the `arraySort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

```
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Here, the elements that are passed in the second array ([2, 1]) define a sorting key for the corresponding element from the source array (['hello', 'world']), that is, ['hello' –> 2, 'world' –> 1]. Since the lambda function doesn't use `x`, actual values of the source array don't affect the order in the result. So, 'hello' will be the second element in the result, and 'world' will be the first.

Other examples are shown below.

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```
``` sql
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```
``` sql
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

!!! note
    To improve sorting efficiency, the [Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform) is used.

## arrayReverseSort([func,] arr, ...) {#array_functions-reverse-sort}

Sorts the elements of the `arr` array in descending order. If the `func` function is specified, `arr` is sorted according to the result of the `func` function applied to the elements of the array, and then the sorted array is reversed. If `func` accepts multiple arguments, the `arrayReverseSort` function is passed several arrays that the arguments of `func` will correspond to. Detailed examples are shown at the end of `arrayReverseSort` description.

Example of integer values sorting:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```
```
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

Example of string values sorting:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```
```
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

Consider the following sorting order for the `NULL`, `NaN` and `Inf` values:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```
``` sql
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

- `Inf` values are first in the array.
- `NULL` values are last in the array.
- `NaN` values are right before `NULL`.
- `-Inf` values are right before `NaN`.

Note that the `arrayReverseSort` is a [higher-order function](higher_order_functions.md). You can pass a lambda function to it as the first argument. Example is shown below.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```
```
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

The array is sorted in the following way:

1. At first, the source array ([1, 2, 3]) is sorted according to the result of the lambda function applied to the elements of the array. The result is an array [3, 2, 1].
2. Array that is obtained on the previous step, is reversed. So, the final result is [1, 2, 3].

The lambda function can accept multiple arguments. In this case, you need to pass the `arrayReverseSort` function several arrays of identical length that the arguments of lambda function will correspond to. The resulting array will consist of elements from the first input array; elements from the next input array(s) specify the sorting keys. For example:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```
``` sql
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

In this example, the array is sorted in the following way:

1. At first, the source array (['hello', 'world']) is sorted according to the result of the lambda function applied to the elements of the arrays. The elements that are passed in the second array ([2, 1]), define the sorting keys for corresponding elements from the source array. The result is an array ['world', 'hello'].
2. Array that was sorted on the previous step, is reversed. So, the final result is ['hello', 'world'].

Other examples are shown below.

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```
``` sql
┌─res─────┐
│ [5,3,4] │
└─────────┘
```
``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```
``` sql
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayUniq(arr, ...)

如果传递一个参数，则计算数组中不同元素的数量。
如果传递了多个参数，则它计算多个数组中相应位置的不同元素元组的数量。

如果要获取数组中唯一项的列表，可以使用arrayReduce（'groupUniqArray'，arr）。

## arrayJoin(arr) {#array_functions-join}

一个特殊的功能。请参见[“ArrayJoin函数”](array_join.md＃functions_arrayjoin)部分。

## arrayDifference(arr)

返回一个数组，其中包含所有相邻元素对之间的差值。例如：

```sql
SELECT arrayDifference([1, 2, 3, 4])
```

```
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

## arrayDistinct(arr)

返回一个包含所有数组中不同元素的数组。例如：

```sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

```
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense(arr)

返回与源数组大小相同的数组，指示每个元素首次出现在源数组中的位置。例如：arrayEnumerateDense（[10,20,10,30]）= [1,2,1,3]。

## arrayIntersect(arr)

返回所有数组元素的交集。例如：

```sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

```
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayReduce(agg_func, arr1, ...)

将聚合函数应用于数组并返回其结果。如果聚合函数具有多个参数，则此函数可应用于相同大小的多个数组。

arrayReduce（'agg_func'，arr1，...） - 将聚合函数`agg_func`应用于数组`arr1 ...`。如果传递了多个数组，则相应位置上的元素将作为多个参数传递给聚合函数。例如：SELECT arrayReduce（'max'，[1,2,3]）= 3

## arrayReverse(arr)

返回与源数组大小相同的数组，包含反转源数组的所有元素的结果。



[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/array_functions/) <!--hide-->
