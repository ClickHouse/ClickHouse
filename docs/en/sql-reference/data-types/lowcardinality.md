---
toc_priority: 51
toc_title: LowCardinality
---

# LowCardinality data type {#lowcardinality-data-type}

Modifies other data types.

## Syntax {#lowcardinality-syntax}

```sql
LowCardinality(data_type)
```

**Parameters**

- `data_type` — Supported ClickHouse [data type](index.md).

## Description {#lowcardinality-dscr}

`LowCardinality` is a superstructure that changes a data storage method and rules of data processing. ClickHouse applies [dictionary coding](https://en.wikipedia.org/wiki/Dictionary_coder) to `LowCardinality`-columns. Operating with dictionary encoded data significantly increases performance of [SELECT](../statements/select/index.md) queries for many applications.

## Related Settings and Functions

Settings:

- low_cardinality_max_dictionary_size
- low_cardinality_use_single_dictionary_for_part
- low_cardinality_allow_in_native_format
- allow_suspicious_low_cardinality_types

Functions:

- toLowCardinality

## See Also

- [String Optimization (video presentation in Russian)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Slides in English](https://github.com/yandex/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf).
- [A Magical Mystery Tour of the LowCardinality Data Type](https://www.altinity.com/blog/2019/3/27/low-cardinality).
