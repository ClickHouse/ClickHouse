# Float32, Float64

[Floating point numbers](https://en.wikipedia.org/wiki/IEEE_754).

这些类型类似于C语言的中类型：

- `Float32` - `float`
- `Float64`  - `double`

我们建议您尽可能以整数形式存储数据例如，将固定精度的数字转换为整数值，例如货币数量或页面加载时间用毫秒为单位表示

## 使用浮点数

- 用浮点数字进行计算可能会产生舍入误差

```sql
SELECT 1 - 0.9
```

```
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- 计算的结果取决于计算方法（计算机系统的处理器类型和体系结构）
- 浮点计算可能会导致诸如无穷大（`INF`）和"非数字"（`NaN`）的数字在处理计算结果时应考虑这一点 
- 当从行读取浮点数时，结果可能不是最接近的机器可表示的数

## NaN and Inf

与标准SQL相比，ClickHouse支持以下类别的浮点数：

- `Inf` – 无穷大

```sql
SELECT 0.5 / 0
```

```
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` – 负无穷大

```sql
SELECT -0.5 / 0
```

```
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` – 非数字

```
SELECT 0 / 0
```

```
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

请参阅本节中的`NaN`排序规则 [ORDER BY clause](../query_language/select.md#query_language-queries-order_by)
