---
description: 'ClickHouse 中 Float32、Float64 和 BFloat16 浮点数据类型的文档'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Float32 | Float64 | BFloat16 类型'
doc_type: 'reference'
---

:::note
如果你需要进行精确计算，尤其是在处理需要高精度的金融或业务数据时，建议考虑改用 [Decimal](../data-types/decimal.md)。

如下所示，[浮点数](https://en.wikipedia.org/wiki/IEEE_754) 可能会导致计算结果不准确：

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

ClickHouse 与 C 中对应的类型如下：

* `Float32` — `float`。
* `Float64` — `double`。

ClickHouse 中的 Float 类型有以下别名：

* `Float32` — `FLOAT`、`REAL`、`SINGLE`。
* `Float64` — `DOUBLE`、`DOUBLE PRECISION`。

创建表时，可以为浮点数指定数值参数 (例如 `FLOAT(12)`、`FLOAT(15, 22)`、`DOUBLE(12)`、`DOUBLE(4, 18)`) ，但 ClickHouse 会忽略这些参数。

<div id="using-floating-point-numbers">
  ## 使用浮点数
</div>

* 浮点数计算可能会产生舍入误差。

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* 计算结果取决于具体的计算方式 (即计算机系统的处理器类型和架构) 。
* 浮点计算可能会得到无穷大 (`Inf`) 和“非数字” (`NaN`) 等结果。处理计算结果时应考虑到这一点。
* 从文本中解析浮点数时，得到的结果可能不是机器可表示的最接近值。

<div id="nan-and-inf">
  ## NaN 和 Inf
</div>

不同于标准 SQL，ClickHouse 支持以下几类浮点数：

* `Inf` – 正无穷。

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — 负无穷大。

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — 非数字。

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

请参见 [ORDER BY 子句](../../sql-reference/statements/select/order-by.md) 一节，了解 `NaN` 的排序规则。

<div id="nan-values-in-set-semantics">
  ## 集合语义中的 NaN 值
</div>

IEEE 754 标准规定，标量比较 `NaN = NaN` 会返回 `false`。
ClickHouse 对 `=` 运算符也遵循这一规则。

不过，`NaN` 并不是单一的值；只要指数全为 1 且
尾数非零，任何位模式都可以表示 `NaN`。不同的操作以及不同的 CPU 架构都可能产生
具有不同符号位或不同尾数载荷的 `NaN` 值。例如：

* `0./0.` 会生成一个 `NaN`，在大多数 x86 平台上其符号位为 1。
* 字面量 `nan` 会生成一个 `NaN`，其符号位为 0。
* 在 [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230) 之后，AArch64 NEON 路径上的
  `log` 对负输入返回的 `NaN`，其符号位与 glibc 的标量 `log` 不同。

ClickHouse 中的哈希表按字节比较键，因此不同的 `NaN` 位模式会被哈希到
不同的桶中，并在 `DISTINCT`、`GROUP BY`、`uniqExact`、`countDistinct` 以及基于 `Float` 键的等值 `JOIN`
等集合语义操作中视为不同的值：

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

这与 IEEE 754 一致 (每个 `NaN` 都不等于任何其他值，包括它本身) ，
但这可能会让人感到意外。如果你需要具有集合语义的操作将所有 `NaN` 值视为相等，
请在查询中先将其规范化：

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

同样的方法也适用于 `DISTINCT`、`GROUP BY` 和 `JOIN` 键。

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` 是一种 16 位浮点数据类型，包含 8 位指数、符号位和 7 位尾数。
它适用于机器学习和 AI 应用。

ClickHouse 支持 `Float32` 与 `BFloat16` 之间的转换，
可使用 [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) 或 [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16) 函数实现。

:::note
大多数其他操作均不支持。
:::