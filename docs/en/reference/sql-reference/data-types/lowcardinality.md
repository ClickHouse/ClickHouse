---
sidebar_position: 51
sidebar_label: LowCardinality
---

# LowCardinality Data Type {#lowcardinality-data-type}

Changes the internal representation of other data types to be dictionary-encoded.

## Syntax {#lowcardinality-syntax}

``` sql
LowCardinality(data_type)
```

**Parameters**

-   `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), and numbers excepting [Decimal](../../sql-reference/data-types/decimal.md). `LowCardinality` is not efficient for some data types, see the [allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types) setting description.

## Description {#lowcardinality-dscr}

`LowCardinality` is a superstructure that changes a data storage method and rules of data processing. ClickHouse applies [dictionary coding](https://en.wikipedia.org/wiki/Dictionary_coder) to `LowCardinality`-columns. Operating with dictionary encoded data significantly increases performance of [SELECT](../../sql-reference/statements/select/index.md) queries for many applications.

The efficiency of using `LowCardinality` data type depends on data diversity. If a dictionary contains less than 10,000 distinct values, then ClickHouse mostly shows higher efficiency of data reading and storing. If a dictionary contains more than 100,000 distinct values, then ClickHouse can perform worse in comparison with using ordinary data types.

Consider using `LowCardinality` instead of [Enum](../../sql-reference/data-types/enum.md) when working with strings. `LowCardinality` provides more flexibility in use and often reveals the same or higher efficiency.

## Example {#example}

Create a table with a `LowCardinality`-column:

``` sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

## Related Settings and Functions {#related-settings-and-functions}

Settings:

-   [low_cardinality_max_dictionary_size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
-   [low_cardinality_use_single_dictionary_for_part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
-   [low_cardinality_allow_in_native_format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
-   [allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
-   [output_format_arrow_low_cardinality_as_dictionary](../../operations/settings/settings.md#output-format-arrow-low-cardinality-as-dictionary)

Functions:

-   [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#tolowcardinality)

## See Also {#see-also}

-   [Reducing ClickHouse Storage Cost with the Low Cardinality Type – Lessons from an Instana Engineer](https://www.instana.com/blog/reducing-clickhouse-storage-cost-with-the-low-cardinality-type-lessons-from-an-instana-engineer/).
-   [String Optimization (video presentation in Russian)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Slides in English](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf).
