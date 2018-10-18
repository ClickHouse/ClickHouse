# Float32, Float64

[浮点数](https://en.wikipedia.org/wiki/IEEE_754)。

类型与以下 C 类型是相同的：

- `Float32` - `float`
- `Float64`  - ` double`

我们建议，如果可能的话尽量用整形来存储数据。比如，将一定精度的浮点数转换成整形，譬如货币金额或者毫秒单位的加载时间。

## 使用浮点数

- 对浮点数进行计算可能引起四舍五入的误差。

   ```sql
   SELECT 1 - 0.9
   ```

   ```
  ┌───────minus(1, 0.9)─┐
  │ 0.09999999999999998 │
  └─────────────────────┘
   ```

- 计算的结果取决于计算方式（处理器类型和计算机系统架构）

- 浮点数计算可能出现这样的结果，比如 "infinity" （`Inf`） 和 "not-a-number" （`NaN`）。对浮点数计算的时候应该考虑到这点。

- 当一行行阅读浮点数的时候，浮点数的结果可能不是机器最近显示的数值。

## NaN 和 Inf

相比于 SQL，ClickHouse 支持以下几种浮点数分类：

- `Inf` – 正无穷。

   ```sql
   SELECT 0.5 / 0
   ```

   ```
  ┌─divide(0.5, 0)─┐
  │            inf │
  └────────────────┘
   ```
- `-Inf` – 负无穷。

   ```sql
   SELECT -0.5 / 0
   ```

   ```
  ┌─divide(-0.5, 0)─┐
  │            -inf │
  └─────────────────┘
   ```
- `NaN` – 非数字。

   ```
   SELECT 0 / 0
   ```

   ```
  ┌─divide(0, 0)─┐
  │          nan │
  └──────────────┘
   ```

可以在[ORDER BY 子句](../query_language/select.md#query_language-queries-order_by) 查看更多关于 ` NaN` 排序的规则。

[来源文章](https://clickhouse.yandex/docs/en/data_types/float/) <!--hide-->
