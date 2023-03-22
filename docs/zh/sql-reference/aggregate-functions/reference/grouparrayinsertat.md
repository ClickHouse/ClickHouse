---
toc_priority: 112
---

# groupArrayInsertAt {#grouparrayinsertat}

在指定位置向数组中插入一个值。

**语法**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

如果在一个查询中将多个值插入到同一位置，则该函数的行为方式如下:

-   如果在单个线程中执行查询，则使用第一个插入的值。
-   如果在多个线程中执行查询，则结果值是未确定的插入值之一。

**参数**

-   `x` — 要插入的值。生成所[支持的数据类型](../../../sql-reference/data-types/index.md)(数据)的[表达式](../../../sql-reference/syntax.md#syntax-expressions)。
-   `pos` — 指定元素 `x` 将被插入的位置。 数组中的索引编号从零开始。 [UInt32](../../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x` — 在空位置替换的默认值。可选参数。生成 `x` 数据类型 (数据) 的[表达式](../../../sql-reference/syntax.md#syntax-expressions)。  如果 `default_x` 未定义，则 [默认值](../../../sql-reference/statements/create.md#create-default-values) 被使用。
-   `size`— 结果数组的长度。可选参数。如果使用该参数，必须指定默认值 `default_x` 。 [UInt32](../../../sql-reference/data-types/int-uint.md#uint-ranges)。

**返回值**

-   具有插入值的数组。

类型: [阵列](../../../sql-reference/data-types/array.md#data-type-array)。

**示例**

查询:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

查询:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

查询:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

结果:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

在一个位置多线程插入数据。

查询:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

作为这个查询的结果，你会得到 `[0,9]` 范围的随机整数。 例如:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```
