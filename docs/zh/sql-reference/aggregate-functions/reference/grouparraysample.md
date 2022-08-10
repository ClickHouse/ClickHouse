---
toc_priority: 114
---

# groupArraySample {#grouparraysample}

构建一个参数值的采样数组。
结果数组的大小限制为 `max_size` 个元素。参数值被随机选择并添加到数组中。

**语法**

``` sql
groupArraySample(max_size[, seed])(x)
```

**参数**

-   `max_size` — 结果数组的最大长度。[UInt64](../../data-types/int-uint.md)。
-   `seed` — 随机数发生器的种子。可选。[UInt64](../../data-types/int-uint.md)。默认值: `123456`。
-   `x` — 参数 (列名 或者 表达式)。

**返回值**

-   随机选取参数 `x` (的值)组成的数组。

类型: [Array](../../../sql-reference/data-types/array.md).

**示例**

样表 `colors`:

``` text
┌─id─┬─color──┐
│  1 │ red    │
│  2 │ blue   │
│  3 │ green  │
│  4 │ white  │
│  5 │ orange │
└────┴────────┘
```

使用列名做参数查询:

``` sql
SELECT groupArraySample(3)(color) as newcolors FROM colors;
```

结果:

```text
┌─newcolors──────────────────┐
│ ['white','blue','green']   │
└────────────────────────────┘
```

使用列名和不同的(随机数)种子查询:

``` sql
SELECT groupArraySample(3, 987654321)(color) as newcolors FROM colors;
```

结果:

```text
┌─newcolors──────────────────┐
│ ['red','orange','green']   │
└────────────────────────────┘
```

使用表达式做参数查询:

``` sql
SELECT groupArraySample(3)(concat('light-', color)) as newcolors FROM colors;
```

结果:

```text
┌─newcolors───────────────────────────────────┐
│ ['light-blue','light-orange','light-green'] │
└─────────────────────────────────────────────┘
```
