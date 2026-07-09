---
description: 'Документация по оптимизации LowCardinality для строковых столбцов'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

Изменяет внутреннее представление других типов данных на словарное кодирование.

<div id="syntax">
  ## Синтаксис
</div>

```sql
LowCardinality(data_type)
```

**Параметры**

* `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [дата и время](../../sql-reference/data-types/datetime.md) и числовые типы, кроме [Decimal](../../sql-reference/data-types/decimal.md). `LowCardinality` малоэффективен для некоторых типов данных, см. описание настройки [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types).

<div id="description">
  ## Описание
</div>

`LowCardinality` — это надстройка, которая изменяет способ хранения данных и правила их обработки. ClickHouse применяет [словарное кодирование](https://en.wikipedia.org/wiki/Dictionary_coder) к столбцам `LowCardinality`. Работа с данными, закодированными по словарю, значительно повышает производительность запросов [SELECT](../../sql-reference/statements/select/index.md) во многих приложениях.

Эффективность использования типа данных `LowCardinality` зависит от разнообразия данных. Если словарь содержит менее 10 000 различных значений, ClickHouse в большинстве случаев обеспечивает более эффективное чтение и хранение данных. Если словарь содержит более 100 000 различных значений, ClickHouse может работать менее эффективно по сравнению с использованием обычных типов данных.

Рекомендуется использовать `LowCardinality` вместо [Enum](../../sql-reference/data-types/enum.md) при работе со строками. `LowCardinality` обеспечивает большую гибкость и нередко оказывается не менее или даже более эффективным.

<div id="example">
  ## Пример
</div>

Создайте таблицу со столбцом `LowCardinality`:

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
  ## Связанные настройки и функции
</div>

Настройки:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/ru/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

Функции:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Оптимизация ClickHouse с помощью схем и кодеков](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Блог: [Работа с временными рядами в ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [Оптимизация String (видеопрезентация на русском)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Слайды на английском](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)