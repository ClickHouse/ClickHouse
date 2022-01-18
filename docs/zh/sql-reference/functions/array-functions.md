# 数组函数 {#shu-zu-han-shu}

## empty {#empty}

对于空数组返回1，对于非空数组返回0。
结果类型是UInt8。
该函数也适用于字符串。

## notEmpty {#notempty}

对于空数组返回0，对于非空数组返回1。
结果类型是UInt8。
该函数也适用于字符串。

## length {#array_functions-length}

返回数组中的元素个数。
结果类型是UInt64。
该函数也适用于字符串。

## emptyArrayUInt8,emptyArrayUInt16,emptyArrayUInt32,emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8,emptyArrayInt16,emptyArrayInt32,emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32,emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate，emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

不接受任何参数并返回适当类型的空数组。

## emptyArrayToSingle {#emptyarraytosingle}

接受一个空数组并返回一个仅包含一个默认值元素的数组。

## range(N) {#rangen}

返回从0到N-1的数字数组。
以防万一，如果在数据块中创建总长度超过100,000,000个元素的数组，则抛出异常。

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

使用函数的参数作为数组元素创建一个数组。
参数必须是常量，并且具有最小公共类型的类型。必须至少传递一个参数，否则将不清楚要创建哪种类型的数组。也就是说，你不能使用这个函数来创建一个空数组（为此，使用上面描述的’emptyArray  \*’函数）。
返回’Array（T）’类型的结果，其中’T’是传递的参数中最小的公共类型。

## arrayConcat {#arrayconcat}

合并参数中传递的所有数组。

    arrayConcat(arrays)

**参数**

-   `arrays` – 任意数量的[阵列](../../sql-reference/functions/array-functions.md)类型的参数.
    **示例**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

    ┌─res───────────┐
    │ [1,2,3,4,5,6] │
    └───────────────┘

## arrayElement(arr,n),运算符arr\[n\] {#arrayelementarr-n-operator-arrn}

从数组`arr`中获取索引为«n»的元素。 `n`必须是任何整数类型。
数组中的索引从一开始。
支持负索引。在这种情况下，它选择从末尾开始编号的相应元素。例如，`arr [-1]`是数组中的最后一项。

如果索引超出数组的边界，则返回默认值（数字为0，字符串为空字符串等）。

## has(arr,elem) {#hasarr-elem}

检查’arr’数组是否具有’elem’元素。
如果元素不在数组中，则返回0;如果在，则返回1。

`NULL` 值的处理。

    SELECT has([1, 2, NULL], NULL)

    ┌─has([1, 2, NULL], NULL)─┐
    │                       1 │
    └─────────────────────────┘

## hasAll {#hasall}

检查一个数组是否是另一个数组的子集。

    hasAll(set, subset)

**参数**

-   `set` – 具有一组元素的任何类型的数组。
-   `subset` – 任何类型的数组，其元素应该被测试为`set`的子集。

**返回值**

-   `1`， 如果`set`包含`subset`中的所有元素。
-   `0`， 否则。

**特殊的定义**

-   空数组是任何数组的子集。
-   «Null»作为数组中的元素值进行处理。
-   忽略两个数组中的元素值的顺序。

**示例**

`SELECT hasAll([], [])` 返回1。

`SELECT hasAll([1, Null], [Null])` 返回1。

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` 返回1。

`SELECT hasAll(['a', 'b'], ['a'])` 返回1。

`SELECT hasAll([1], ['a'])` 返回0。

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` 返回0。

## hasAny {#hasany}

检查两个数组是否存在交集。

    hasAny(array1, array2)

**参数**

-   `array1` – 具有一组元素的任何类型的数组。
-   `array2` – 具有一组元素的任何类型的数组。

**返回值**

-   `1`， 如果`array1`和`array2`存在交集。
-   `0`， 否则。

**特殊的定义**

-   «Null»作为数组中的元素值进行处理。
-   忽略两个数组中的元素值的顺序。

**示例**

`SELECT hasAny([1], [])` 返回 `0`.

`SELECT hasAny([Null], [Null, 1])` 返回 `1`.

`SELECT hasAny([-128, 1., 512], [1])` 返回 `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` 返回 `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` 返回 `1`.

## indexOf(arr,x) {#indexofarr-x}

返回数组中第一个’x’元素的索引（从1开始），如果’x’元素不存在在数组中，则返回0。

示例:

    :) SELECT indexOf([1,3,NULL,NULL],NULL)

    SELECT indexOf([1, 3, NULL, NULL], NULL)

    ┌─indexOf([1, 3, NULL, NULL], NULL)─┐
    │                                 3 │
    └───────────────────────────────────┘

设置为«NULL»的元素将作为普通的元素值处理。

## countEqual(arr,x) {#countequalarr-x}

返回数组中等于x的元素的个数。相当于arrayCount（elem - \> elem = x，arr）。

`NULL`值将作为单独的元素值处理。

示例:

    SELECT countEqual([1, 2, NULL, NULL], NULL)

    ┌─countEqual([1, 2, NULL, NULL], NULL)─┐
    │                                    2 │
    └──────────────────────────────────────┘

## arrayEnumerate(arr) {#array_functions-arrayenumerate}

返回 Array \[1, 2, 3, …, length (arr) \]

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

    ┌─Reaches─┬──Hits─┐
    │   95606 │ 31406 │
    └─────────┴───────┘

在此示例中，Reaches是转换次数（应用ARRAY JOIN后接收的字符串），Hits是浏览量（ARRAY JOIN之前的字符串）。在这种特殊情况下，您可以更轻松地获得相同的结果：

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

    ┌─Reaches─┬──Hits─┐
    │   95606 │ 31406 │
    └─────────┴───────┘

此功能也可用于高阶函数。例如，您可以使用它来获取与条件匹配的元素的数组索引。

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

返回与源数组大小相同的数组，其中每个元素表示与其下标对应的源数组元素在源数组中出现的次数。
例如：arrayEnumerateUniq（ \[10,20,10,30 \]）=  \[1,1,2,1 \]。

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

在此示例中，每个GoalID都计算转换次数（目标嵌套数据结构中的每个元素都是达到的目标，我们称之为转换）和会话数。如果没有ARRAY JOIN，我们会将会话数计为总和（Sign）。但在这种特殊情况下，行乘以嵌套的Goals结构，因此为了在此之后计算每个会话一次，我们将一个条件应用于arrayEnumerateUniq（Goals.ID）函数的值。

arrayEnumerateUniq函数可以使用与参数大小相同的多个数组。在这种情况下，对于所有阵列中相同位置的元素元组，考虑唯一性。

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

    ┌─res───────────┐
    │ [1,2,1,1,2,1] │
    └───────────────┘

当使用带有嵌套数据结构的ARRAY JOIN并在此结构中跨多个元素进一步聚合时，这是必需的。

## arrayPopBack {#arraypopback}

从数组中删除最后一项。

    arrayPopBack(array)

**参数**

-   `array` – 数组。

**示例**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

    ┌─res───┐
    │ [1,2] │
    └───────┘

## arrayPopFront {#arraypopfront}

从数组中删除第一项。

    arrayPopFront(array)

**参数**

-   `array` – 数组。

**示例**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

    ┌─res───┐
    │ [2,3] │
    └───────┘

## arrayPushBack {#arraypushback}

添加一个元素到数组的末尾。

    arrayPushBack(array, single_value)

**参数**

-   `array` – 数组。
-   `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅«[数据类型](../../sql-reference/functions/array-functions.md#data_types)»。可以是’NULL`。该函数向数组添加一个«NULL»元素，数组元素的类型转换为`Nullable\`。

**示例**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

    ┌─res───────┐
    │ ['a','b'] │
    └───────────┘

## arrayPushFront {#arraypushfront}

将一个元素添加到数组的开头。

    arrayPushFront(array, single_value)

**参数**

-   `array` – 数组。
-   `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅«[数据类型](../../sql-reference/functions/array-functions.md#data_types)»。可以是’NULL`。该函数向数组添加一个«NULL»元素，数组元素的类型转换为`Nullable\`。

**示例**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

    ┌─res───────┐
    │ ['a','b'] │
    └───────────┘

## arrayResize {#arrayresize}

更改数组的长度。

    arrayResize(array, size[, extender])

**参数:**

-   `array` — 数组.
-   `size` — 数组所需的长度。
    -   如果`size`小于数组的原始大小，则数组将从右侧截断。
-   如果`size`大于数组的初始大小，则使用`extender`值或数组项的数据类型的默认值将数组扩展到右侧。
-   `extender` — 扩展数组的值。可以是’NULL\`。

**返回值:**

一个`size`长度的数组。

**调用示例**

    SELECT arrayResize([1], 3)

    ┌─arrayResize([1], 3)─┐
    │ [1,0,0]             │
    └─────────────────────┘

    SELECT arrayResize([1], 3, NULL)

    ┌─arrayResize([1], 3, NULL)─┐
    │ [1,NULL,NULL]             │
    └───────────────────────────┘

## arraySlice {#arrayslice}

返回一个子数组，包含从指定位置的指定长度的元素。

    arraySlice(array, offset[, length])

**参数**

-   `array` – 数组。
-   `offset` – 数组的偏移。正值表示左侧的偏移量，负值表示右侧的缩进值。数组下标从1开始。
-   `length` - 子数组的长度。如果指定负值，则该函数返回`[offset，array_length  -  length`。如果省略该值，则该函数返回`[offset，the_end_of_array]`。

**示例**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

    ┌─res────────┐
    │ [2,NULL,4] │
    └────────────┘

设置为«NULL»的数组元素作为普通的数组元素值处理。

## arraySort(\[func,\] arr, …) {#array_functions-reverse-sort}

以升序对`arr`数组的元素进行排序。如果指定了`func`函数，则排序顺序由`func`函数的调用结果决定。如果`func`接受多个参数，那么`arraySort`函数也将解析与`func`函数参数相同数量的数组参数。更详细的示例在`arraySort`的末尾。

整数排序示例:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

    ┌─arraySort([1, 3, 3, 0])─┐
    │ [0,1,3,3]               │
    └─────────────────────────┘

字符串排序示例:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

    ┌─arraySort(['hello', 'world', '!'])─┐
    │ ['!','hello','world']              │
    └────────────────────────────────────┘

`NULL`，`NaN`和`Inf`的排序顺序：

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

    ┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
    │ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
    └───────────────────────────────────────────────────────────┘

-   `-Inf` 是数组中的第一个。
-   `NULL` 是数组中的最后一个。
-   `NaN` 在`NULL`的前面。
-   `Inf` 在`NaN`的前面。

注意：`arraySort`是[高阶函数](higher-order-functions.md)。您可以将lambda函数作为第一个参数传递给它。在这种情况下，排序顺序由lambda函数的调用结果决定。

让我们来看一下如下示例：

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

    ┌─res─────┐
    │ [3,2,1] │
    └─────────┘

对于源数组的每个元素，lambda函数返回排序键，即\[1 -\> -1, 2 -\> -2, 3 -\> -3\]。由于`arraySort`函数按升序对键进行排序，因此结果为\[3,2,1\]。因此，`(x) -> -x` lambda函数将排序设置为[降序](#array_functions-reverse-sort)。

lambda函数可以接受多个参数。在这种情况下，您需要为`arraySort`传递与lambda参数个数相同的数组。函数使用第一个输入的数组中的元素组成返回结果；使用接下来传入的数组作为排序键。例如：

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

    ┌─res────────────────┐
    │ ['world', 'hello'] │
    └────────────────────┘

这里，在第二个数组（\[2, 1\]）中定义了第一个数组（\[‘hello’，‘world’\]）的相应元素的排序键，即\[‘hello’ -\> 2，‘world’ -\> 1\]。 由于lambda函数中没有使用`x`，因此源数组中的实际值不会影响结果的顺序。所以，’world’将是结果中的第一个元素，’hello’将是结果中的第二个元素。

其他示例如下所示。

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

!!! 注意 "注意"
    为了提高排序效率， 使用了[施瓦茨变换](https://en.wikipedia.org/wiki/Schwartzian_transform)。

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

以降序对`arr`数组的元素进行排序。如果指定了`func`函数，则排序顺序由`func`函数的调用结果决定。如果`func`接受多个参数，那么`arrayReverseSort`函数也将解析与`func`函数参数相同数量的数组作为参数。更详细的示例在`arrayReverseSort`的末尾。

整数排序示例:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

    ┌─arrayReverseSort([1, 3, 3, 0])─┐
    │ [3,3,1,0]                      │
    └────────────────────────────────┘

字符串排序示例:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

    ┌─arrayReverseSort(['hello', 'world', '!'])─┐
    │ ['world','hello','!']                     │
    └───────────────────────────────────────────┘

`NULL`，`NaN`和`Inf`的排序顺序：

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` sql
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` 是数组中的第一个。
-   `NULL` 是数组中的最后一个。
-   `NaN` 在`NULL`的前面。
-   `-Inf` 在`NaN`的前面。

注意：`arraySort`是[高阶函数](higher-order-functions.md)。您可以将lambda函数作为第一个参数传递给它。如下示例所示。

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

    ┌─res─────┐
    │ [1,2,3] │
    └─────────┘

数组按以下方式排序：
数组按以下方式排序:

1.  首先，根据lambda函数的调用结果对源数组（\[1, 2, 3\]）进行排序。 结果是\[3, 2, 1\]。
2.  反转上一步获得的数组。 所以，最终的结果是\[1, 2, 3\]。

lambda函数可以接受多个参数。在这种情况下，您需要为`arrayReverseSort`传递与lambda参数个数相同的数组。函数使用第一个输入的数组中的元素组成返回结果；使用接下来传入的数组作为排序键。例如：

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` sql
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

在这个例子中，数组按以下方式排序：

1.  首先，根据lambda函数的调用结果对源数组（\[‘hello’，‘world’\]）进行排序。 其中，在第二个数组（\[2,1\]）中定义了源数组中相应元素的排序键。 所以，排序结果\[‘world’，‘hello’\]。
2.  反转上一步骤中获得的排序数组。 所以，最终的结果是\[‘hello’，‘world’\]。

其他示例如下所示。

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

## arrayUniq(arr, …) {#arrayuniqarr}

如果传递一个参数，则计算数组中不同元素的数量。
如果传递了多个参数，则它计算多个数组中相应位置的不同元素元组的数量。

如果要获取数组中唯一项的列表，可以使用arrayReduce（‘groupUniqArray’，arr）。

## arryjoin(arr) {#array-functions-join}

一个特殊的功能。请参见[«ArrayJoin函数»](array-join.md#functions_arrayjoin)部分。

## arrayDifference(arr) {#arraydifferencearr}

返回一个数组，其中包含所有相邻元素对之间的差值。例如：

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

    ┌─arrayDifference([1, 2, 3, 4])─┐
    │ [0,1,1,1]                     │
    └───────────────────────────────┘

## arrayDistinct(arr) {#arraydistinctarr}

返回一个包含所有数组中不同元素的数组。例如：

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

    ┌─arrayDistinct([1, 2, 2, 3, 1])─┐
    │ [1,2,3]                        │
    └────────────────────────────────┘

## arrayEnumerateDense(arr) {#arrayenumeratedensearr}

返回与源数组大小相同的数组，指示每个元素首次出现在源数组中的位置。例如：arrayEnumerateDense（\[10,20,10,30\]）= \[1,2,1,3\]。

## arrayIntersect(arr) {#arrayintersectarr}

返回所有数组元素的交集。例如：

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

    ┌─no_intersect─┬─intersect─┐
    │ []           │ [1]       │
    └──────────────┴───────────┘

## arrayReduce(agg_func, arr1, …) {#arrayreduceagg-func-arr1}

将聚合函数应用于数组并返回其结果。如果聚合函数具有多个参数，则此函数可应用于相同大小的多个数组。

arrayReduce（‘agg_func’，arr1，…） - 将聚合函数`agg_func`应用于数组`arr1 ...`。如果传递了多个数组，则相应位置上的元素将作为多个参数传递给聚合函数。例如：SELECT arrayReduce（‘max’，\[1,2,3\]）= 3

## arrayReverse(arr) {#arrayreversearr}

返回与源数组大小相同的数组，包含反转源数组的所有元素的结果。

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
