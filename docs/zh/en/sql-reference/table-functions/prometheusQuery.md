---
description: '使用 TimeSeries 表中的数据来计算 Prometheus 查询结果。'
sidebar_label: 'prometheusQuery'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQuery
title: 'prometheusQuery'
doc_type: 'reference'
---

使用 TimeSeries 表中的数据来计算 Prometheus 查询结果。

<div id="syntax">
  ## 语法
</div>

```sql
prometheusQuery('db_name', 'time_series_table', 'promql_query', evaluation_time)
prometheusQuery(db_name.time_series_table, 'promql_query', evaluation_time)
prometheusQuery('time_series_table', 'promql_query', evaluation_time)
```

<div id="arguments">
  ## 参数
</div>

* `db_name` - TimeSeries 表所在的数据库名称。
* `time_series_table` - TimeSeries 表的名称。
* `promql_query` - 使用 [PromQL 语法](https://prometheus.io/docs/prometheus/latest/querying/basics/) 编写的查询。
* `evaluation_time - 求值时间戳。若要按当前时间对查询求值，请使用 `now()`作为`evaluation&#95;time&#96;。

<div id="returned_value">
  ## 返回值
</div>

该函数会根据传递给参数 `promql_query` 的查询结果类型，返回不同的列：

| 结果类型   | 结果列                                                                                       | 示例                                                  |
| ------ | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## PromQL 支持的功能
</div>

<div id="selectors">
  ### 选择器
</div>

即时选择器、范围选择器、标签匹配器 (`=`、`!=`、`=~`、`!~`) 、`offset` 修饰符、`@` 时间戳修饰符以及子查询。

<div id="functions">
  ### 函数
</div>

| 类别   | 函数                                                                                               |
| ---- | ------------------------------------------------------------------------------------------------ |
| 范围   | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| 数学   | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| 三角函数 | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| 日期时间 | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| 类型   | `scalar`, `vector`                                                                               |
| 直方图  | `histogram_quantile`                                                                             |
| 其他   | `time`, `pi`                                                                                     |

**注**：`histogram_quantile` 对经典直方图桶 (由 `le` 标签标识) 使用线性插值。原生直方图目前尚不受支持，且 `phi` (分位数级别) 参数当前必须是常量标量——像 `histogram_quantile(time() / 1000, ...)` 这样按每个步长变化的表达式会因 `NOT_IMPLEMENTED` 错误而被拒绝。

<div id="operators">
  ### 运算符
</div>

所有算术 (`+`、`-`、`*`、`/`、`%`、`^`) 、比较 (`==`、`!=`、`<`、`>`、`<=`、`>=`，可带可选的 `bool`) 和逻辑 (`and`、`or`、`unless`) 二元运算符，以及 `on()`/`ignoring()` 和 `group_left()`/`group_right()` 修饰符。

一元运算符 `+` 和 `-`。

<div id="aggregation-operators">
  ### 聚合运算符
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` —— 可选 `by()` 或 `without()` 修饰符。

尚不支持：`count_values`。

<div id="example">
  ## 示例
</div>

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```