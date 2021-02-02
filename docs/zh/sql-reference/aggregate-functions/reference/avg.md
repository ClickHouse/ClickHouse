---
toc_priority: 5
---

# avg {#agg_function-avg}

计算算术平均值。

**语法**

``` sql
avg(x)
```

**参数**

-   `x` — 列名

`x` 必须是
[Integer](../../../sql-reference/data-types/int-uint.md),
[floating-point](../../../sql-reference/data-types/float.md), or 
[Decimal](../../../sql-reference/data-types/decimal.md).

**返回值**

- `NaN`。 参数列为空时返回。
- 算术平均值。 其他情况。

**返回类型** 总是 [Float64](../../../sql-reference/data-types/float.md).

**示例**

查询:

``` sql
SELECT avg(x) FROM values('x Int8', 0, 1, 2, 3, 4, 5)
```

结果:

``` text
┌─avg(x)─┐
│    2.5 │
└────────┘
```

**示例**

查询:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
SELECT avg(t) FROM test
```

结果:

``` text
┌─avg(x)─┐
│    nan │
└────────┘
```
