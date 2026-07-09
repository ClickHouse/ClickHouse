---
alias: []
description: 'Документация по формату ORC'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

[Apache ORC](https://orc.apache.org/) — столбцовый формат хранения данных, широко используемый в экосистеме [Hadoop](https://hadoop.apache.org/).

<div id="data-types-matching-orc">
  ## Соответствие типов данных
</div>

В таблице ниже сравниваются поддерживаемые типы данных ORC и соответствующие им [типы данных](/ru/sql-reference/data-types/index.md) ClickHouse в запросах `INSERT` и `SELECT`.

| Тип данных ORC (`INSERT`)             | Тип данных ClickHouse                                                                             | Тип данных ORC (`SELECT`) |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------- |
| `Boolean`                             | [UInt8](/ru/sql-reference/data-types/int-uint.md)                                                    | `Boolean`                 |
| `Tinyint`                             | [Int8/UInt8](/ru/sql-reference/data-types/int-uint.md)/[Enum8](/ru/sql-reference/data-types/enum.md)    | `Tinyint`                 |
| `Smallint`                            | [Int16/UInt16](/ru/sql-reference/data-types/int-uint.md)/[Enum16](/ru/sql-reference/data-types/enum.md) | `Smallint`                |
| `Int`                                 | [Int32/UInt32](/ru/sql-reference/data-types/int-uint.md)                                             | `Int`                     |
| `Bigint`                              | [Int64/UInt32](/ru/sql-reference/data-types/int-uint.md)                                             | `Bigint`                  |
| `Float`                               | [Float32](/ru/sql-reference/data-types/float.md)                                                     | `Float`                   |
| `Double`                              | [Float64](/ru/sql-reference/data-types/float.md)                                                     | `Double`                  |
| `Decimal`                             | [Decimal](/ru/sql-reference/data-types/decimal.md)                                                   | `Decimal`                 |
| `Date`                                | [Date32](/ru/sql-reference/data-types/date32.md)                                                     | `Date`                    |
| `Timestamp`                           | [DateTime64](/ru/sql-reference/data-types/datetime64.md)                                             | `Timestamp`               |
| `String`, `Char`, `Varchar`, `Binary` | [String](/ru/sql-reference/data-types/string.md)                                                     | `Binary`                  |
| `List`                                | [Array](/ru/sql-reference/data-types/array.md)                                                       | `List`                    |
| `Struct`                              | [Tuple](/ru/sql-reference/data-types/tuple.md)                                                       | `Struct`                  |
| `Map`                                 | [Map](/ru/sql-reference/data-types/map.md)                                                           | `Map`                     |
| `Int`                                 | [IPv4](/ru/sql-reference/data-types/int-uint.md)                                                     | `Int`                     |
| `Binary`                              | [IPv6](/ru/sql-reference/data-types/ipv6.md)                                                         | `Binary`                  |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/ru/sql-reference/data-types/int-uint.md)                            | `Binary`                  |
| `Binary`                              | [Decimal256](/ru/sql-reference/data-types/decimal.md)                                                | `Binary`                  |

* Другие типы не поддерживаются.
* Массивы могут быть вложенными и могут иметь значение типа `Nullable` в качестве аргумента. Типы `Tuple` и `Map` также могут быть вложенными.
* Типы данных столбцов таблицы ClickHouse не обязаны совпадать с соответствующими полями ORC. При вставке данных ClickHouse интерпретирует типы данных в соответствии с таблицей выше, а затем [приводит](/ru/sql-reference/functions/type-conversion-functions#CAST) данные к типу, заданному для столбца таблицы ClickHouse.

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-data">
  ### Вставка данных
</div>

Используйте ORC-файл с приведёнными ниже данными под именем `football.orc`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Вставьте данные:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### Чтение данных
</div>

Прочитайте данные в формате `ORC`:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC — это бинарный формат, который не отображается в терминале в человекочитаемом виде. Используйте `INTO OUTFILE` для вывода файлов ORC.
:::

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                                                                                            | Описание                                                                                          | По умолчанию |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------ |
| [`output_format_arrow_string_as_string`](/ru/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Использовать тип Arrow String вместо Binary для столбцов типа String.                             | `false`      |
| [`output_format_orc_compression_method`](/ru/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Метод сжатия, используемый для вывода в формате ORC. Значение по умолчанию                        | `none`       |
| [`input_format_arrow_case_insensitive_column_matching`](/ru/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Игнорировать регистр при сопоставлении столбцов Arrow со столбцами ClickHouse.                    | `false`      |
| [`input_format_arrow_allow_missing_columns`](/ru/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Разрешить отсутствие столбцов при чтении данных Arrow.                                            | `false`      |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/ru/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Разрешить пропуск столбцов с неподдерживаемыми типами при автоматическом определении схемы Arrow. | `false`      |

Для обмена данными с Hadoop можно использовать [движок таблицы HDFS](/ru/engines/table-engines/integrations/hdfs.md).