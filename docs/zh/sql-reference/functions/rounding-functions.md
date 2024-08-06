---
slug: /zh/sql-reference/functions/rounding-functions
---
# 取整函数 {#qu-zheng-han-shu}

## 楼(x\[,N\]) {#floorx-n}

返回小于或等于x的最大舍入数。该函数使用参数乘1/10N，如果1/10N不精确，则选择最接近的精确的适当数据类型的数。
’N’是一个整数常量，可选参数。默认为0，这意味着不对其进行舍入。
’N’可以是负数。

示例: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x`是任何数字类型。结果与其为相同类型。
对于整数参数，使用负’N’值进行舍入是有意义的（对于非负«N»，该函数不执行任何操作）。
如果取整导致溢出（例如，floor(-128，-1)），则返回特定于实现的结果。

## ceil(x\[,N\]),天花板(x\[,N\]) {#ceilx-n-ceilingx-n}

返回大于或等于’x’的最小舍入数。在其他方面，它与’floor’功能相同（见上文）。

## trunc(x\[, N\]), truncate(x\[, N\])

返回绝对值小于或等于 `x` 的绝对值最大的整数。在其他方面，它与 "floor "函数相同（见上文）。

**语法**

```SQL
trunc(input, precision)
```

别名：`truncate`。

**参数***

- `输入` 数值类型（[Float](/docs/en/sql-reference/data-types/float.md)、[Decimal](/docs/en/sql-reference/data-types/decimal.md)或[Integer](/docs/en/sql-reference/data-types/int-uint.md)）。
- `精度` 一个 [Integer](/docs/en/sql-reference/data-types/int-uint.md) 类型。

**返回值**

- 输入 "的数据类型。

**示例

查询：

``` SQL
SELECT trunc(123.499, 1) as res；
```

```responce
┌──res─┐
│ 123.4 │
└───────┘
```

## 圆形(x\[,N\]) {#rounding_functions-round}

将值取整到指定的小数位数。

该函数按顺序返回最近的数字。如果给定数字包含多个最近数字，则函数返回其中最接近偶数的数字（银行的取整方式）。

    round(expression [, decimal_places])

**参数：**

-   `expression` — 要进行取整的数字。可以是任何返回数字[类型](../../sql-reference/functions/rounding-functions.md#data_types)的[表达式](../syntax.md#syntax-expressions)。
-   `decimal-places` — 整数类型。
    -   如果`decimal-places > 0`，则该函数将值舍入小数点右侧。
    -   如果`decimal-places < 0`，则该函数将小数点左侧的值四舍五入。
    -   如果`decimal-places = 0`，则该函数将该值舍入为整数。在这种情况下，可以省略参数。

**返回值：**

与输入数字相同类型的取整后的数字。

### 示例 {#shi-li}

**使用示例**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

    ┌───x─┬─round(divide(number, 2))─┐
    │   0 │                        0 │
    │ 0.5 │                        0 │
    │   1 │                        1 │
    └─────┴──────────────────────────┘

**取整的示例**

取整到最近的数字。

    round(3.2, 0) = 3
    round(4.1267, 2) = 4.13
    round(22,-1) = 20
    round(467,-2) = 500
    round(-467,-2) = -500

银行的取整。

    round(3.5) = 4
    round(4.5) = 4
    round(3.55, 1) = 3.6
    round(3.65, 1) = 3.6

## roundToExp2(num) {#roundtoexp2num}

接受一个数字。如果数字小于1，则返回0。否则，它将数字向下舍入到最接近的（整个非负）2的x次幂。

## 圆形饱和度(num) {#rounddurationnum}

接受一个数字。如果数字小于1，则返回0。否则，它将数字向下舍入为集合中的数字：1，10，30，60，120，180，240，300，600，1200，1800，3600，7200，18000，36000。此函数用于Yandex.Metrica报表中计算会话的持续时长。

## 圆数(num) {#roundagenum}

接受一个数字。如果数字小于18，则返回0。否则，它将数字向下舍入为集合中的数字：18，25，35，45，55。此函数用于Yandex.Metrica报表中用户年龄的计算。

## roundDown(num,arr) {#rounddownnum-arr}

接受一个数字，将其向下舍入到指定数组中的元素。如果该值小于数组中的最低边界，则返回最低边界。
