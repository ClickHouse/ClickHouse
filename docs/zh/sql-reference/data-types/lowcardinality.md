---
toc_priority: 51
toc_title: 低基数类型
---

# 低基数类型 {#lowcardinality-data-type}

把其它数据类型转变为字典编码类型。

## 语法 {#lowcardinality-syntax}

```sql
LowCardinality(data_type)
```

**参数**

- `data_type` — [String](string.md), [FixedString](fixedstring.md), [Date](date.md), [DateTime](datetime.md)，包括数字类型，但是[Decimal](decimal.md)除外。对一些数据类型来说，`LowCardinality` 并不高效，详查[allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)设置描述。

## 描述 {#lowcardinality-dscr}

`LowCardinality` 是一种改变数据存储和数据处理方法的概念。 ClickHouse会把 `LowCardinality` 所在的列进行[dictionary coding](https://en.wikipedia.org/wiki/Dictionary_coder)。对很多应用来说，处理字典编码的数据可以显著的增加[SELECT](../statements/select/index.md)查询速度。

使用 `LowCarditality` 数据类型的效率依赖于数据的多样性。如果一个字典包含少于10000个不同的值，那么ClickHouse可以进行更高效的数据存储和处理。反之如果字典多于10000，效率会表现的更差。

当使用字符类型的时候，可以考虑使用 `LowCardinality` 代替[Enum](enum.md)。 `LowCardinality` 通常更加灵活和高效。

## 例子

创建一个 `LowCardinality` 类型的列：

```sql
CREATE TABLE lc_t
(
    `id` UInt16, 
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

## 相关的设置和函数

设置:

- [low_cardinality_max_dictionary_size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
- [low_cardinality_use_single_dictionary_for_part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
- [low_cardinality_allow_in_native_format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
- [allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)

函数:

- [toLowCardinality](../functions/type-conversion-functions.md#tolowcardinality)

## 参考

- [高效低基数类型](https://www.altinity.com/blog/2019/3/27/low-cardinality).
- [使用低基数类型减少ClickHouse的存储成本 – 来自Instana工程师的分享](https://www.instana.com/blog/reducing-clickhouse-storage-cost-with-the-low-cardinality-type-lessons-from-an-instana-engineer/).
- [字符优化 (俄语视频分享)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [英语分享](https://github.com/yandex/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf).