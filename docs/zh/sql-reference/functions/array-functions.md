---
slug: /zh/sql-reference/functions/array-functions
---
# 数组函数 {#shu-zu-han-shu}

## empty {#empty函数}

检查输入的数组是否为空。

**语法**

``` sql
empty([x])
```

如果一个数组中不包含任何元素，则此数组为空数组。

:::注意    
可以通过启用[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns)配置进行优化。设置`optimize_functions_to_subcolumns = 1`后，函数通过读取[size0](../../sql-reference/data-types/array.md#array-size)子列获取结果，不在读取和处理整个数组列，查询语句`SELECT empty(arr) FROM TABLE;`将转化为`SELECT arr.size0 = 0 FROM TABLE;`。
:::

此函数也适用于[strings](string-functions.md#empty)或[UUID](uuid-functions.md#empty)。

**参数**

-   `[x]` — 输入的数组，类型为[数组](../data-types/array.md)。

**返回值**

-   对于空数组返回`1`，对于非空数组返回`0`。

类型: [UInt8](../data-types/int-uint.md)。

**示例**

查询语句:

```sql
SELECT empty([]);
```

结果:

```text
┌─empty(array())─┐
│              1 │
└────────────────┘
```

## notEmpty {#notempty函数}

检测输入的数组是否非空。

**语法**

``` sql
notEmpty([x])
```

如果一个数组至少包含一个元素，则此数组为非空数组。

:::注意    
可以通过启用[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns)配置进行优化。设置`optimize_functions_to_subcolumns = 1`后，函数通过读取[size0](../../sql-reference/data-types/array.md#array-size)子列获取结果，不在读取和处理整个数组列，查询语句`SELECT notEmpty(arr) FROM TABLE;`将转化为`SELECT arr.size0 != 0 FROM TABLE;`。
:::

此函数也适用于[strings](string-functions.md#empty)或[UUID](uuid-functions.md#empty)。

**参数**

-   `[x]` — 输入的数组，类型为[数组](../data-types/array.md)。

**返回值**

-   对于空数组返回`0`，对于非空数组返回`1`。

类型: [UInt8](../data-types/int-uint.md).

**示例**

查询语句:

```sql
SELECT notEmpty([1,2]);
```

结果:

```text
┌─notEmpty([1, 2])─┐
│                1 │
└──────────────────┘
```

## length {#array_functions-length}

返回数组中的元素个数。
结果类型是UInt64。
该函数也适用于字符串。

可以通过启用[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns)配置进行优化。设置`optimize_functions_to_subcolumns = 1`后，函数通过读取[size0](../../sql-reference/data-types/array.md#array-size)子列获取结果，不在读取和处理整个数组列，查询语句`SELECT length(arr) FROM table`将转化为`SELECT arr.size0 FROM TABLE`。

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32, emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

不接受任何参数并返回适当类型的空数组。

## emptyArrayToSingle {#emptyarraytosingle}

接受一个空数组并返回一个仅包含一个默认值元素的数组。

## range(end), range(\[start, \] end \[, step\]) {#range}

返回一个以`step`作为增量步长的从`start`到`end - 1`的整形数字数组， 支持类型包括[`UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`](../data-types/int-uint.md)。

**语法**
``` sql
range([start, ] end [, step])
```

**参数**

-   `start` — 数组的第一个元素。可选项，如果设置了`step`时同样需要`start`，默认值为：0。
-   `end` — 计数到`end`结束，但不包括`end`，必填项。
-   `step` — 确定数组中每个元素之间的增量步长。可选项，默认值为：1。

**返回值**

-   以`step`作为增量步长的从`start`到`end - 1`的数字数组。

**注意事项**

-   所有参数`start`、`end`、`step`必须属于以下几种类型之一：[`UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`](../data-types/int-uint.md)。结果数组的元素数据类型为所有入参类型的最小超类，也必须属于以上几种类型之一。
-   如果查询结果的数组总长度超过[function_range_max_elements_in_block](../../operations/settings/settings.md#settings-function_range_max_elements_in_block)指定的元素数，将会抛出异常。

**示例**

查询语句:
``` sql
SELECT range(5), range(1, 5), range(1, 5, 2), range(-1, 5, 2);
```
结果:
```txt
┌─range(5)────┬─range(1, 5)─┬─range(1, 5, 2)─┬─range(-1, 5, 2)─┐
│ [0,1,2,3,4] │ [1,2,3,4]   │ [1,3]          │ [-1,1,3]        │
└─────────────┴─────────────┴────────────────┴─────────────────┘
```

## array(x1, ...), operator \[x1, ...\] {#arrayx1-operator-x1}

使用函数的参数作为数组元素创建一个数组。
参数必须是常量，并且具有最小公共类型的类型。必须至少传递一个参数，否则将不清楚要创建哪种类型的数组。也就是说，你不能使用这个函数来创建一个空数组（为此，使用上面描述的’emptyArray  \*’函数）。
返回’Array（T）’类型的结果，其中’T’是传递的参数中最小的公共类型。

## arrayConcat {#arrayconcat}

合并参数中传递的所有数组。

``` sql
arrayConcat(arrays)
```

**参数**

-   `arrays` – 任意数量的[阵列](../../sql-reference/functions/array-functions.md)类型的参数.
    **示例**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

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

## hasSubstr {#hassubstr}

检查 array2 的所有元素是否以相同的顺序出现在 array1 中。当且仅当 `array1 = prefix + array2 + suffix`时，该函数将返回 1。

``` sql
hasSubstr(array1, array2)
```

换句话说，函数将检查 `array2` 的所有元素是否都包含在 `array1` 中，就像 `hasAll` 函数一样。此外，它将检查元素在“array1”和“array2”中的出现顺序是否相同。

举例:
- `hasSubstr([1,2,3,4], [2,3])`返回`1`。然而`hasSubstr([1,2,3,4], [3,2])`将返回`0`。
- `hasSubstr([1,2,3,4], [1,2,3])`返回`1`。然而`hasSubstr([1,2,3,4], [1,2,4])`将返回`0`。

**参数**

-   `array1` – 具有一组元素的任何类型数组。
-   `array2` – 具有一组元素的任何类型数组。

**返回值**

-   如果`array1`中包含`array2`，返回`1`。
-   其他情况返回`0`。

**特殊属性**

-   当`array2`为空数组时，函数将返回`1`.
-   `Null` 作为值处理。换句话说，`hasSubstr([1, 2, NULL, 3, 4], [2,3])` 将返回 `0`。但是，`hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` 将返回 `1`。
-   两个数组中**值的顺序**会影响函数结果。

**示例**

`SELECT hasSubstr([], [])`返回1。

`SELECT hasSubstr([1, Null], [Null])`返回1。

`SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])`返回0。

`SELECT hasSubstr(['a', 'b'], ['a'])`返回1。

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])`返回1。

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])`返回0。

`SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])`返回1。

## indexOf(arr,x) {#indexofarr-x}

返回数组中第一个’x’元素的索引（从1开始），如果’x’元素不存在在数组中，则返回0。

示例:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

设置为«NULL»的元素将作为普通的元素值处理。

## arrayCount(\[func,\] arr1, ...) {#array-count}

`func`将arr数组作为参数，其返回结果为非零值的数量。如果未指定“func”，则返回数组中非零元素的数量。

请注意，`arrayCount`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您可以将 lambda 函数作为第一个参数传递给它。

## countEqual(arr,x) {#countequalarr-x}

返回数组中等于x的元素的个数。相当于arrayCount（elem - \> elem = x，arr）。

`NULL`值将作为单独的元素值处理。

示例:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
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

``` text
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

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

此功能也可用于高阶函数。例如，您可以使用它来获取与条件匹配的元素的数组索引。

## arrayEnumerateUniq(arr, ...) {#arrayenumerateuniqarr}

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

在此示例中，每个GoalID都计算转换次数（目标嵌套数据结构中的每个元素都是达到的目标，我们称之为转换）和会话数。如果没有ARRAY JOIN，我们会将会话数计为总和（Sign）。但在这种特殊情况下，行乘以嵌套的Goals结构，因此为了在此之后计算每个会话一次，我们将一个条件应用于arrayEnumerateUniq（Goals.ID）函数的值。

arrayEnumerateUniq函数可以使用与参数大小相同的多个数组。在这种情况下，对于所有阵列中相同位置的元素元组，考虑唯一性。

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

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

``` text
┌─res───┐
│ [1,2] │
└───────┘
```
## arrayPopFront {#arraypopfront}

从数组中删除第一项。

    arrayPopFront(array)

**参数**

-   `array` – 数组。

**示例**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

添加一个元素到数组的末尾。

``` sql
arrayPushBack(array, single_value)
```

**参数**

-   `array` – 数组。
-   `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅«[数据类型](../../sql-reference/functions/array-functions.md#data_types)»。可以是’NULL`。该函数向数组添加一个«NULL»元素，数组元素的类型转换为`Nullable\`。

**示例**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront {#arraypushfront}

将一个元素添加到数组的开头。

``` sql
arrayPushFront(array, single_value)
```

**参数**

-   `array` – 数组。
-   `single_value` – 单个值。只能将数字添加到带数字的数组中，并且只能将字符串添加到字符串数组中。添加数字时，ClickHouse会自动为数组的数据类型设置`single_value`类型。有关ClickHouse中数据类型的更多信息，请参阅«[数据类型](../../sql-reference/functions/array-functions.md#data_types)»。可以是’NULL`。该函数向数组添加一个«NULL»元素，数组元素的类型转换为`Nullable\`。

**示例**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

更改数组的长度。

``` sql
arrayResize(array, size[, extender])
```

**参数:**

-   `array` — 数组.
-   `size` — 数组所需的长度。
    -   如果`size`小于数组的原始大小，则数组将从右侧截断。
-   如果`size`大于数组的初始大小，则使用`extender`值或数组项的数据类型的默认值将数组扩展到右侧。
-   `extender` — 扩展数组的值。可以是’NULL\`。

**返回值:**

一个`size`长度的数组。

**调用示例**

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

## arraySlice {#arrayslice}

返回一个子数组，包含从指定位置的指定长度的元素。

``` sql
arraySlice(array, offset[, length])
```

**参数**

- `array` – 数组。
- `offset` – 数组的偏移。正值表示左侧的偏移量，负值表示右侧的缩进值。数组下标从1开始。
- `length` - 子数组的长度。如果指定负值，则该函数返回`[offset，array_length - length]`。如果省略该值，则该函数返回`[offset，the_end_of_array]`。

**示例**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

设置为«NULL»的数组元素作为普通的数组元素值处理。

## arraySort(\[func,\] arr, ...) {#array_functions-reverse-sort}

以升序对`arr`数组的元素进行排序。如果指定了`func`函数，则排序顺序由`func`函数的调用结果决定。如果`func`接受多个参数，那么`arraySort`函数也将解析与`func`函数参数相同数量的数组参数。更详细的示例在`arraySort`的末尾。

整数排序示例:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

字符串排序示例:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

`NULL`，`NaN`和`Inf`的排序顺序：

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` 是数组中的第一个。
-   `NULL` 是数组中的最后一个。
-   `NaN` 在`NULL`的前面。
-   `Inf` 在`NaN`的前面。

注意：`arraySort`是[高阶函数](higher-order-functions.md)。您可以将lambda函数作为第一个参数传递给它。在这种情况下，排序顺序由lambda函数的调用结果决定。

让我们来看一下如下示例：

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

对于源数组的每个元素，lambda函数返回排序键，即\[1 -\> -1, 2 -\> -2, 3 -\> -3\]。由于`arraySort`函数按升序对键进行排序，因此结果为\[3,2,1\]。因此，`(x) -> -x` lambda函数将排序设置为[降序](#array_functions-reverse-sort)。

lambda函数可以接受多个参数。在这种情况下，您需要为`arraySort`传递与lambda参数个数相同的数组。函数使用第一个输入的数组中的元素组成返回结果；使用接下来传入的数组作为排序键。例如：

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

这里，在第二个数组（\[2, 1\]）中定义了第一个数组（\[‘hello’，‘world’\]）的相应元素的排序键，即\[‘hello’ -\> 2，‘world’ -\> 1\]。 由于lambda函数中没有使用`x`，因此源数组中的实际值不会影响结果的顺序。所以，’world’将是结果中的第一个元素，’hello’将是结果中的第二个元素。

其他示例如下所示。

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

!!! 注意 "注意"
    为了提高排序效率， 使用了[施瓦茨变换](https://en.wikipedia.org/wiki/Schwartzian_transform)。

## arrayReverseSort(\[func,\] arr, ...) {#array_functions-reverse-sort}

以降序对`arr`数组的元素进行排序。如果指定了`func`函数，则排序顺序由`func`函数的调用结果决定。如果`func`接受多个参数，那么`arrayReverseSort`函数也将解析与`func`函数参数相同数量的数组作为参数。更详细的示例在`arrayReverseSort`的末尾。

整数排序示例:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

字符串排序示例:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

`NULL`，`NaN`和`Inf`的排序顺序：

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
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

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

数组按以下方式排序:

1.  首先，根据lambda函数的调用结果对源数组（\[1, 2, 3\]）进行排序。 结果是\[3, 2, 1\]。
2.  反转上一步获得的数组。 所以，最终的结果是\[1, 2, 3\]。

lambda函数可以接受多个参数。在这种情况下，您需要为`arrayReverseSort`传递与lambda参数个数相同的数组。函数使用第一个输入的数组中的元素组成返回结果；使用接下来传入的数组作为排序键。例如：

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
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

## arrayUniq(arr, ...) {#arrayuniqarr}

如果传递一个参数，则计算数组中不同元素的数量。
如果传递了多个参数，则它计算多个数组中相应位置的不同元素元组的数量。

如果要获取数组中唯一项的列表，可以使用arrayReduce（‘groupUniqArray’，arr）。

## arrayJoin(arr) {#array-functions-join}

一个特殊的功能。请参见[«ArrayJoin函数»](array-join.md#functions_arrayjoin)部分。

## arrayDifference {#arraydifference}

计算相邻数组元素之间的差异。返回一个数组，其中第一个元素为 0，第二个是 `a[1] - a[0]` 之差等。结果数组中元素的类型由减法的类型推断规则确定（例如`UInt8` - `UInt8` = `Int16`）。

**语法**

``` sql
arrayDifference(array)
```

**参数**

-   `array` –类型为[数组](https://clickhouse.com/docs/en/data_types/array/)。

**返回值**

返回相邻元素之间的差异数组。

类型: [UInt\*](https://clickhouse.com/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.com/docs/en/data_types/int_uint/#int-ranges), [Float\*](https://clickhouse.com/docs/en/data_types/float/)。

**示例**

查询语句:

``` sql
SELECT arrayDifference([1, 2, 3, 4]);
```

结果:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

由于结果类型为Int64导致的溢出示例:

查询语句:

``` sql
SELECT arrayDifference([0, 10000000000000000000]);
```

结果:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```
## arrayDistinct {#arraydistinctarr}

返回一个包含所有数组中不同元素的数组。

**语法**

``` sql
arrayDistinct(array)
```

**参数**

-   `array` –类型为[数组](https://clickhouse.com/docs/en/data_types/array/)。

**返回值**

返回一个包含不同元素的数组。

**示例**

查询语句:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1]);
```

结果:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense(arr) {#arrayenumeratedensearr}

返回与源数组大小相同的数组，指示每个元素首次出现在源数组中的位置。例如：arrayEnumerateDense（\[10,20,10,30\]）= \[1,2,1,3\]。

示例:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## arrayIntersect(arr) {#array-functions-arrayintersect}

返回所有数组元素的交集。例如：

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

将聚合函数应用于数组元素并返回其结果。聚合函数的名称以单引号 `'max'`、`'sum'` 中的字符串形式传递。使用参数聚合函数时，参数在括号中的函数名称后指示“uniqUpTo(6)”。

**语法**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**参数**

-   `agg_func` — 聚合函数的名称，应该是一个常量[string](../../sql-reference/data-types/string.md)。
-   `arr` — 任意数量的[数组](../../sql-reference/data-types/array.md)类型列作为聚合函数的参数。

**返回值**

**示例**

查询语句:

``` sql
SELECT arrayReduce('max', [1, 2, 3]);
```

结果:

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

如果聚合函数采用多个参数，则该函数必须应用于多个相同大小的数组。

查询语句:

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0]);
```

结果:

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

带有参数聚合函数的示例：

查询语句:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
```

结果:

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceInRanges {#arrayreduceinranges}

将聚合函数应用于给定范围内的数组元素，并返回一个包含与每个范围对应的结果的数组。该函数将返回与多个 `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)` 相同的结果。

**语法**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**参数**

-   `agg_func` — 聚合函数的名称，应该是一个常量[string](../../sql-reference/data-types/string.md)。
-   `ranges` — 要聚合的范围应该是[元组](../../sql-reference/data-types/tuple.md)为元素的[数组](../../sql-reference/data-types/array.md)，其中包含每个索引和长度范围。
-   `arr` — 任意数量的[数组](../../sql-reference/data-types/array.md)类型列作为聚合函数的参数。

**返回值**

-   包含指定范围内聚合函数结果的数组。

类型为: [数组](../../sql-reference/data-types/array.md).

**示例**

查询语句:

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

结果:

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## arrayReverse(arr) {#arrayreverse}

返回一个与原始数组大小相同的数组，其中包含相反顺序的元素。

示例:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## reverse(arr) {#array-functions-reverse}

与[“arrayReverse”](#arrayreverse)作用相同。

## arrayFlatten {#arrayflatten}

将嵌套的数组展平。

函数:

-   适用于任何深度的嵌套数组。
-   不会更改已经展平的数组。

展平后的数组包含来自所有源数组的所有元素。

**语法**

``` sql
flatten(array_of_arrays)
```

别名: `flatten`.

**参数**

-   `array_of_arrays` — 嵌套[数组](../../sql-reference/data-types/array.md)。 例如：`[[1,2,3], [4,5]]`。

**示例**

``` sql
SELECT flatten([[[1]], [[2], [3]]]);
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact {#arraycompact}

从数组中删除连续的重复元素。结果值的顺序由源数组中的顺序决定。

**语法**

``` sql
arrayCompact(arr)
```

**参数**

`arr` — 类型为[数组](../../sql-reference/data-types/array.md)。

**返回值**

没有重复元素的数组。

类型为: `Array`。

**示例**

查询语句:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3]);
```

结果:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

将多个数组组合成一个数组。结果数组包含按列出的参数顺序分组为元组的源数组的相应元素。

**语法**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**参数**

-   `arrN` — N个[数组](../../sql-reference/data-types/array.md)。

该函数可以采用任意数量的不同类型的数组。所有输入数组的大小必须相等。

**返回值**
  
- 将源数组中的元素分组为[元组](../../sql-reference/data-types/tuple.md)的数组。元组中的数据类型与输入数组的类型相同，并且与传递数组的顺序相同。

类型为: [数组](../../sql-reference/data-types/array.md)。

**示例**

查询语句:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1]);
```

结果:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayAUC {#arrayauc}

计算AUC (ROC曲线下的面积，这是机器学习中的一个概念，更多细节请查看：https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve)。

**语法**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**参数**

- `arr_scores` — 分数预测模型给出。
- `arr_labels` — 样本的标签，通常为 1 表示正样本，0 表示负样本。

**返回值**

返回 Float64 类型的 AUC 值。

**示例**

查询语句:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
```

结果:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└───────────────────────────────────────────────┘
```

## arrayMap(func, arr1, ...) {#array-map}

将从 `func` 函数的原始应用中获得的数组返回给 `arr` 数组中的每个元素。

示例:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

下面的例子展示了如何从不同的数组创建一个元素的元组：

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

请注意，`arrayMap` 是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayFilter(func, arr1, ...) {#array-filter}

返回一个仅包含 `arr1` 中的元素的数组，其中 `func` 返回的值不是 0。

示例:

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

请注意，`arrayFilter`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayFill(func, arr1, ...) {#array-fill}

从第一个元素到最后一个元素扫描`arr1`，如果`func`返回0，则用`arr1[i - 1]`替换`arr1[i]`。`arr1`的第一个元素不会被替换。

示例:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

请注意，`arrayFill` 是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayReverseFill(func, arr1, ...) {#array-reverse-fill}

从最后一个元素到第一个元素扫描`arr1`，如果`func`返回0，则用`arr1[i + 1]`替换`arr1[i]`。`arr1`的最后一个元素不会被替换。

示例:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

请注意，`arrayReverseFill`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arraySplit(func, arr1, ...) {#array-split}

将 `arr1` 拆分为多个数组。当 `func` 返回 0 以外的值时，数组将在元素的左侧拆分。数组不会在第一个元素之前被拆分。

示例:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

请注意，`arraySplit`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayReverseSplit(func, arr1, ...) {#array-reverse-split}

将 `arr1` 拆分为多个数组。当 `func` 返回 0 以外的值时，数组将在元素的右侧拆分。数组不会在最后一个元素之后被拆分。

示例:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

请注意，`arrayReverseSplit`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。 您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayExists(\[func,\] arr1, ...) {#arrayexistsfunc-arr1}

如果 `arr` 中至少有一个元素 `func` 返回 0 以外的值，则返回 1。否则，它返回 0。

请注意，`arrayExists`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayAll(\[func,\] arr1, ...) {#arrayallfunc-arr1}

如果 `func` 为 `arr` 中的所有元素返回 0 以外的值，则返回 1。否则，它返回 0。

请注意，`arrayAll`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayFirst(func, arr1, ...) {#array-first}

返回 `arr1` 数组中 `func` 返回非 0 的值的第一个元素。

请注意，`arrayFirst`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayLast(func, arr1, ...) {#array-last}

返回 `arr1` 数组中的最后一个元素，其中 `func` 返回的值不是 0。

请注意，`arrayLast`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayFirstIndex(func, arr1, ...) {#array-first-index}

返回 `arr1` 数组中第一个元素的索引，其中 `func` 返回的值不是 0。

请注意，`arrayFirstIndex`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayLastIndex(func, arr1, ...) {#array-last-index}

返回 `arr1` 数组中最后一个元素的索引，其中 `func` 返回的值不是 0。

请注意，`arrayLastIndex`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您必须将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayMin {#array-min}

返回源数组中的最小元素。

如果指定了 `func` 函数，则返回此函数转换的元素的最小值。

请注意，`arrayMin`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions)。您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

**语法**

```sql
arrayMin([func,] arr)
```

**参数**

-   `func` — 类型为[表达式](../../sql-reference/data-types/special-data-types/expression.md)。
-   `arr` — 类型为[数组](../../sql-reference/data-types/array.md)。

**返回值**

-   函数值的最小值（或数组最小值）。

类型：如果指定了`func`，则匹配`func`返回值类型，否则匹配数组元素类型。

**示例**

查询语句:

```sql
SELECT arrayMin([1, 2, 4]) AS res;
```

结果:

```text
┌─res─┐
│   1 │
└─────┘
```

查询语句:

```sql
SELECT arrayMin(x -> (-x), [1, 2, 4]) AS res;
```

结果:

```text
┌─res─┐
│  -4 │
└─────┘
```

## arrayMax {#array-max}

返回源数组中元素的最大值。

如果指定了`func` 函数，则返回此函数转换的元素的最大值。

请注意，`arrayMax`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions). 您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

**语法**

```sql
arrayMax([func,] arr)
```

**参数**

-   `func` — 类型为[表达式](../../sql-reference/data-types/special-data-types/expression.md)。
-   `arr` — 类型为[数组](../../sql-reference/data-types/array.md)。

**返回值**

-   函数值的最大值（或数组最大值）。

类型：如果指定了`func`，则匹配`func`返回值类型，否则匹配数组元素类型。

**示例**

查询语句：

```sql
SELECT arrayMax([1, 2, 4]) AS res;
```

结果：

```text
┌─res─┐
│   4 │
└─────┘
```

查询语句：

```sql
SELECT arrayMax(x -> (-x), [1, 2, 4]) AS res;
```

结果：

```text
┌─res─┐
│  -1 │
└─────┘
```

## arraySum {#array-sum}

返回源数组中元素的总和。

如果指定了 `func` 函数，则返回此函数转换的元素的总和。

请注意，`arraySum`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions). 您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

**语法**

```sql
arraySum([func,] arr)
```

**参数**

-   `func` — 类型为[表达式](../../sql-reference/data-types/special-data-types/expression.md)。
-   `arr` — 类型为[数组](../../sql-reference/data-types/array.md)。

**返回值**

-   函数值的总和（或数组总和）。

类型：源数组中的十进制数（或转换后的值，如果指定了`func`）-[Decimal128](../../sql-reference/data-types/decimal.md)， 对于浮点数 — [Float64](../../sql-reference/data-types/float.md)， 对于无符号数 — [UInt64](../../sql-reference/data-types/int-uint.md)， 对于有符号数 — [Int64](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句：

```sql
SELECT arraySum([2, 3]) AS res;
```

结果：

```text
┌─res─┐
│   5 │
└─────┘
```

查询语句：

```sql
SELECT arraySum(x -> x*x, [2, 3]) AS res;
```

结果：

```text
┌─res─┐
│  13 │
└─────┘
```

## arrayAvg {#array-avg}

返回源数组中元素的平均值。

如果指定了 func 函数，则返回此函数转换的元素的平均值。

请注意，`arrayAvg`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions). 您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

**语法**

```sql
arrayAvg([func,] arr)
```

**参数**

-   `func` — 类型为[表达式](../../sql-reference/data-types/special-data-types/expression.md)。
-   `arr` — 类型为[数组](../../sql-reference/data-types/array.md)。

**返回值**

-   函数值的平均值（或数组平均值）。

类型为: [Float64](../../sql-reference/data-types/float.md).

**示例**

查询语句：

```sql
SELECT arrayAvg([1, 2, 4]) AS res;
```

结果：

```text
┌────────────────res─┐
│ 2.3333333333333335 │
└────────────────────┘
```

查询语句：

```sql
SELECT arrayAvg(x -> (x * x), [2, 4]) AS res;
```

结果：

```text
┌─res─┐
│  10 │
└─────┘
```

## arrayCumSum(\[func,\] arr1, ...) {#arraycumsumfunc-arr1}

返回源数组中元素的部分和的数组（运行总和）。如果指定了 func 函数，则数组元素的值在求和之前由该函数转换。

示例:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

请注意，`arrayCumSum`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions). 您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayCumSumNonNegative(arr) {#arraycumsumnonnegativearr}

与 `arrayCumSum` 相同，返回源数组中元素的部分和的数组（运行总和）。不同的`arrayCumSum`，当返回值包含小于零的值时，将该值替换为零，并以零参数执行后续计算。例如：

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```
请注意`arraySumNonNegative`是一个[高阶函数](../../sql-reference/functions/index.md#higher-order-functions). 您可以将 lambda 函数作为第一个参数传递给它，并且不能省略。

## arrayProduct {#arrayproduct}

将一个[数组](../../sql-reference/data-types/array.md)中的元素相乘。

**语法**

``` sql
arrayProduct(arr)
```

**参数**

-   `arr` — 数值类型的[数组](../../sql-reference/data-types/array.md)。

**返回值**

-   数组元素的乘积。

类型为: [Float64](../../sql-reference/data-types/float.md).

**示例**

查询语句：

``` sql
SELECT arrayProduct([1,2,3,4,5,6]) as res;
```

结果：

``` text
┌─res───┐
│ 720   │
└───────┘
```

查询语句：

``` sql
SELECT arrayProduct([toDecimal64(1,8), toDecimal64(2,8), toDecimal64(3,8)]) as res, toTypeName(res);
```

返回值类型总是[Float64](../../sql-reference/data-types/float.md). 结果：

``` text
┌─res─┬─toTypeName(arrayProduct(array(toDecimal64(1, 8), toDecimal64(2, 8), toDecimal64(3, 8))))─┐
│ 6   │ Float64                                                                                  │
└─────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```
