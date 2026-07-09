---
alias: []
description: 'توثيق تنسيق ORC'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'reference'
---

| إدخال | إخراج | الاسم البديل |
| ----- | ----- | ------------ |
| ✔     | ✔     |              |

<div id="description">
  ## الوصف
</div>

يُعد [Apache ORC](https://orc.apache.org/) تنسيق تخزين عمودي يُستخدم على نطاق واسع في منظومة [Hadoop](https://hadoop.apache.org/).

<div id="data-types-matching-orc">
  ## مطابقة أنواع البيانات
</div>

يقارن الجدول أدناه بين أنواع بيانات ORC المدعومة وما يقابلها من [أنواع البيانات](/ar/sql-reference/data-types/index.md) في ClickHouse ضمن استعلامات `INSERT` و`SELECT`.

| نوع بيانات ORC (`INSERT`)             | نوع بيانات ClickHouse                                                                             | نوع بيانات ORC (`SELECT`) |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------- |
| `Boolean`                             | [UInt8](/ar/sql-reference/data-types/int-uint.md)                                                    | `Boolean`                 |
| `Tinyint`                             | [Int8/UInt8](/ar/sql-reference/data-types/int-uint.md)/[Enum8](/ar/sql-reference/data-types/enum.md)    | `Tinyint`                 |
| `Smallint`                            | [Int16/UInt16](/ar/sql-reference/data-types/int-uint.md)/[Enum16](/ar/sql-reference/data-types/enum.md) | `Smallint`                |
| `Int`                                 | [Int32/UInt32](/ar/sql-reference/data-types/int-uint.md)                                             | `Int`                     |
| `Bigint`                              | [Int64/UInt32](/ar/sql-reference/data-types/int-uint.md)                                             | `Bigint`                  |
| `Float`                               | [Float32](/ar/sql-reference/data-types/float.md)                                                     | `Float`                   |
| `Double`                              | [Float64](/ar/sql-reference/data-types/float.md)                                                     | `Double`                  |
| `Decimal`                             | [Decimal](/ar/sql-reference/data-types/decimal.md)                                                   | `Decimal`                 |
| `Date`                                | [Date32](/ar/sql-reference/data-types/date32.md)                                                     | `Date`                    |
| `Timestamp`                           | [DateTime64](/ar/sql-reference/data-types/datetime64.md)                                             | `Timestamp`               |
| `String`, `Char`, `Varchar`, `Binary` | [String](/ar/sql-reference/data-types/string.md)                                                     | `Binary`                  |
| `List`                                | [Array](/ar/sql-reference/data-types/array.md)                                                       | `List`                    |
| `Struct`                              | [Tuple](/ar/sql-reference/data-types/tuple.md)                                                       | `Struct`                  |
| `Map`                                 | [Map](/ar/sql-reference/data-types/map.md)                                                           | `Map`                     |
| `Int`                                 | [IPv4](/ar/sql-reference/data-types/int-uint.md)                                                     | `Int`                     |
| `Binary`                              | [IPv6](/ar/sql-reference/data-types/ipv6.md)                                                         | `Binary`                  |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/ar/sql-reference/data-types/int-uint.md)                            | `Binary`                  |
| `Binary`                              | [Decimal256](/ar/sql-reference/data-types/decimal.md)                                                | `Binary`                  |

* الأنواع الأخرى غير مدعومة.
* يمكن أن تكون المصفوفات متداخلة، ويمكن أن تقبل قيمة من النوع `Nullable` كوسيطة. ويمكن أيضًا أن يكون النوعان `Tuple` و`Map` متداخلين.
* لا يلزم أن تتطابق أنواع بيانات أعمدة جدول ClickHouse مع حقول بيانات ORC المقابلة. عند إدراج البيانات، يفسّر ClickHouse أنواع البيانات وفقًا للجدول أعلاه ثم [يحوّلها](/ar/sql-reference/functions/type-conversion-functions#CAST) إلى نوع البيانات المحدد لعمود جدول ClickHouse.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف ORC يحتوي على البيانات التالية، باسم `football.orc`:

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

أدرِج البيانات:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات بتنسيق `ORC`:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC هو تنسيق ثنائي لا يظهر على الطرفية بصيغة قابلة للقراءة البشرية. استخدم `INTO OUTFILE` لإخراج ملفات ORC.
:::

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                                                                                              | الوصف                                                                            | الافتراضي |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | --------- |
| [`output_format_arrow_string_as_string`](/ar/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | استخدام النوع Arrow String بدلًا من Binary للأعمدة من نوع String.                | `false`   |
| [`output_format_orc_compression_method`](/ar/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | طريقة الضغط المستخدمة في تنسيق ORC للإخراج. القيمة الافتراضية                    | `none`    |
| [`input_format_arrow_case_insensitive_column_matching`](/ar/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | تجاهل حالة الأحرف عند مطابقة أعمدة Arrow مع أعمدة ClickHouse.                    | `false`   |
| [`input_format_arrow_allow_missing_columns`](/ar/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | السماح بالأعمدة المفقودة أثناء قراءة بيانات Arrow.                               | `false`   |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/ar/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | السماح بتخطي الأعمدة ذات الأنواع غير المدعومة أثناء استدلال المخطط لتنسيق Arrow. | `false`   |

لتبادل البيانات مع Hadoop، يمكنك استخدام [محرك الجداول HDFS](/ar/engines/table-engines/integrations/hdfs.md).