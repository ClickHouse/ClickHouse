---
alias: []
description: 'وثائق تنسيق BSONEachRow'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'مرجع'
---

| الإدخال | الناتج | الاسم البديل |
| ------- | ------ | ------------ |
| ✔       | ✔      |              |

<div id="description">
  ## الوصف
</div>

يقوم التنسيق `BSONEachRow` بتحليل البيانات على هيئة تسلسل من مستندات JSON الثنائية (BSON) من دون أي فاصل بينها.
يُنسَّق كل صف على شكل مستند واحد، ويُنسَّق كل عمود على شكل حقل واحد في مستند BSON، ويُستخدم اسم العمود كمفتاح.

<div id="data-types-matching">
  ## مطابقة أنواع البيانات
</div>

بالنسبة إلى الإخراج، تُستخدم المطابقة التالية بين أنواع ClickHouse وأنواع BSON:

| نوع ClickHouse                                                                                        | نوع BSON                                                                                                                            |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| [Bool](/ar/sql-reference/data-types/boolean.md)                                                          | `\x08` منطقي                                                                                                                      |
| [Int8/UInt8](/ar/sql-reference/data-types/int-uint.md)/[Enum8](/ar/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                        |
| [Int16/UInt16](/ar/sql-reference/data-types/int-uint.md)/[Enum16](/ar/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                        |
| [Int32](/ar/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                        |
| [UInt32](/ar/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                        |
| [Int64/UInt64](/ar/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                        |
| [Float32/Float64](/ar/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                       |
| [Date](/ar/sql-reference/data-types/date.md)/[Date32](/ar/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                        |
| [DateTime](/ar/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                        |
| [DateTime64](/ar/sql-reference/data-types/datetime64.md)                                                 | `\x09` تاريخ ووقت                                                                                                                     |
| [Decimal32](/ar/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                        |
| [Decimal64](/ar/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                        |
| [Decimal128](/ar/sql-reference/data-types/decimal.md)                                                    | `\x05` ثنائي، `\x00` ثنائي subtype، الحجم = 16                                                                                    |
| [Decimal256](/ar/sql-reference/data-types/decimal.md)                                                    | `\x05` ثنائي، `\x00` ثنائي subtype، الحجم = 32                                                                                    |
| [Int128/UInt128](/ar/sql-reference/data-types/int-uint.md)                                               | `\x05` ثنائي، `\x00` ثنائي subtype، الحجم = 16                                                                                    |
| [Int256/UInt256](/ar/sql-reference/data-types/int-uint.md)                                               | `\x05` ثنائي، `\x00` ثنائي subtype، الحجم = 32                                                                                    |
| [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md) | `\x05` ثنائي، `\x00` ثنائي subtype أو \x02 string إذا كان الإعداد output&#95;format&#95;bson&#95;string&#95;as&#95;string مفعّلًا |
| [UUID](/ar/sql-reference/data-types/uuid.md)                                                             | `\x05` ثنائي، `\x04` uuid subtype، الحجم = 16                                                                                      |
| [Array](/ar/sql-reference/data-types/array.md)                                                           | `\x04` مصفوفة                                                                                                                        |
| [Tuple](/ar/sql-reference/data-types/tuple.md)                                                           | `\x04` مصفوفة                                                                                                                        |
| [Named Tuple](/ar/sql-reference/data-types/tuple.md)                                                     | `\x03` مستند                                                                                                                     |
| [Map](/ar/sql-reference/data-types/map.md)                                                               | `\x03` مستند                                                                                                                     |
| [IPv4](/ar/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                        |
| [IPv6](/ar/sql-reference/data-types/ipv6.md)                                                             | `\x05` ثنائي، `\x00` ثنائي subtype                                                                                                |

بالنسبة إلى الإدخال، تُستخدم المطابقة التالية بين أنواع BSON وأنواع ClickHouse:

| نوع BSON                                         | نوع ClickHouse                                                                                                                                                                                      |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                                    | [Float32/Float64](/ar/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` string                                    | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` مستند                                     | [Map](/ar/sql-reference/data-types/map.md)/[Named Tuple](/ar/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` مصفوفة                                    | [Array](/ar/sql-reference/data-types/array.md)/[Tuple](/ar/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` ثنائي، `\x00` النوع الفرعي الثنائي        | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)/[IPv6](/ar/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` ثنائي، `\x02` النوع الفرعي الثنائي القديم | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` ثنائي، `\x03` النوع الفرعي القديم لـ uuid | [UUID](/ar/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` ثنائي، `\x04` النوع الفرعي لـ uuid        | [UUID](/ar/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                                  | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` منطقي                                     | [Bool](/ar/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` تاريخ ووقت                                | [DateTime64](/ar/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` قيمة NULL                                 | [NULL](/ar/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` شيفرة JavaScript                          | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` رمز                                       | [String](/ar/sql-reference/data-types/string.md)/[FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                                     | [Int32/UInt32](/ar/sql-reference/data-types/int-uint.md)/[Decimal32](/ar/sql-reference/data-types/decimal.md)/[IPv4](/ar/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/ar/sql-reference/data-types/enum.md) |
| `\x12` int64                                     | [Int64/UInt64](/ar/sql-reference/data-types/int-uint.md)/[Decimal64](/ar/sql-reference/data-types/decimal.md)/[DateTime64](/ar/sql-reference/data-types/datetime64.md)                                       |

أنواع BSON الأخرى غير مدعومة. بالإضافة إلى ذلك، يُجري تحويلًا بين أنواع الأعداد الصحيحة المختلفة.
على سبيل المثال، يمكن إدراج قيمة BSON من النوع `int32` في ClickHouse بوصفها [`UInt8`](../../sql-reference/data-types/int-uint.md).

يمكن تحليل الأعداد الصحيحة الكبيرة والقيم العشرية مثل `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` من قيمة BSON ثنائية ذات النوع الفرعي الثنائي `\x00`.
في هذه الحالة، سيتحقق التنسيق من أن حجم البيانات الثنائية يساوي حجم القيمة المتوقعة.

:::note
لا يعمل هذا التنسيق على نحو صحيح على المنصات ذات ترتيب البايتات Big-Endian.
:::

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف BSON بالبيانات التالية، باسم `football.bson`:

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
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام تنسيق `BSONEachRow`:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON هي صيغة ثنائية لا تظهر في الطرفية بصيغة مقروءة للبشر. استخدم `INTO OUTFILE` لإخراج ملفات BSON.
:::

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                                                                                               | الوصف                                                                              | الافتراضي |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | --------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | استخدم نوع BSON String بدلًا من Binary لأعمدة String.                              | `false`   |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | اسمح بتخطي الأعمدة ذات الأنواع غير المدعومة عند استدلال المخطط لتنسيق BSONEachRow. | `false`   |