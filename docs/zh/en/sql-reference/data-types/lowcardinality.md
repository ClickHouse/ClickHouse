---
description: 'String 列的 LowCardinality 优化文档'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

将其他数据类型的内部表示改为字典编码形式。

<div id="syntax">
  ## 语法
</div>

```sql
LowCardinality(data_type)
```

**参数**

* `data_type` — [String](../../sql-reference/data-types/string.md)、[FixedString](../../sql-reference/data-types/fixedstring.md)、[Date](../../sql-reference/data-types/date.md)、[日期时间](../../sql-reference/data-types/datetime.md)，以及除 [Decimal](../../sql-reference/data-types/decimal.md) 之外的数值类型。`LowCardinality` 对某些数据类型并不高效，参见 [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types) 的设置说明。

<div id="description">
  ## 说明
</div>

`LowCardinality` 是一种会改变数据存储方式和数据处理规则的封装类型。ClickHouse 会对 `LowCardinality` 列应用[字典编码](https://en.wikipedia.org/wiki/Dictionary_coder)。对于许多应用来说，对经过字典编码的数据进行操作可以显著提升 [SELECT](../../sql-reference/statements/select/index.md) 查询的性能。

使用 `LowCardinality` 数据类型的效果取决于数据中不同值的数量。如果字典包含的不同值少于 10,000 个，ClickHouse 在数据读取和存储方面通常会更高效。如果字典包含的不同值超过 100,000 个，那么与使用普通数据类型相比，ClickHouse 的性能反而可能更差。

处理字符串时，可以考虑用 `LowCardinality` 代替 [Enum](../../sql-reference/data-types/enum.md)。`LowCardinality` 使用起来更灵活，而且通常能达到相同甚至更高的效率。

<div id="example">
  ## 示例
</div>

创建一个包含 `LowCardinality` 列的表：

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## 相关设置和函数
</div>

设置：

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/zh/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

函数：

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## 相关内容
</div>

* 博客：[使用 schema 和编解码器优化 ClickHouse](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* 博客：[在 ClickHouse 中处理时间序列数据](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [String 优化 (俄语视频演讲) ](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt)。[英文幻灯片](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)