---
alias: []
description: 'توثيق لتنسيق Arrow'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يأتي [Apache Arrow](https://arrow.apache.org/) مزوّدًا بتنسيقين مدمجين للتخزين العمودي.
يدعم ClickHouse عمليات القراءة والكتابة لهذين التنسيقين.
ويُعد `Arrow` تنسيق &quot;وضع الملف&quot; في Apache Arrow، وهو مصمم للوصول العشوائي في الذاكرة.

<div id="data-types-matching">
  ## مطابقة أنواع البيانات
</div>

يوضح الجدول أدناه أنواع البيانات المدعومة وكيف تقابل [أنواع البيانات](/ar/sql-reference/data-types/index.md) في ClickHouse في استعلامات `INSERT` و`SELECT`.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                             | Arrow data type (`SELECT`) |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `BOOL`                                  | [Bool](/ar/sql-reference/data-types/boolean.md)                                                                     | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/ar/sql-reference/data-types/int-uint.md)                                                                   | `UINT8`                    |
| `INT8`                                  | [Int8](/ar/sql-reference/data-types/int-uint.md)/[Enum8](/ar/sql-reference/data-types/enum.md)                         | `INT8`                     |
| `UINT16`                                | [UInt16](/ar/sql-reference/data-types/int-uint.md)                                                                  | `UINT16`                   |
| `INT16`                                 | [Int16](/ar/sql-reference/data-types/int-uint.md)/[Enum16](/ar/sql-reference/data-types/enum.md)                       | `INT16`                    |
| `UINT32`                                | [UInt32](/ar/sql-reference/data-types/int-uint.md)                                                                  | `UINT32`                   |
| `INT32`                                 | [Int32](/ar/sql-reference/data-types/int-uint.md)                                                                   | `INT32`                    |
| `UINT64`                                | [UInt64](/ar/sql-reference/data-types/int-uint.md)                                                                  | `UINT64`                   |
| `INT64`                                 | [Int64](/ar/sql-reference/data-types/int-uint.md)                                                                   | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/ar/sql-reference/data-types/float.md)                                                                    | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/ar/sql-reference/data-types/float.md)                                                                    | `FLOAT64`                  |
| `DATE32`                                | [Date32](/ar/sql-reference/data-types/date32.md)                                                                    | `UINT16`                   |
| `DATE64`                                | [DateTime](/ar/sql-reference/data-types/datetime.md)                                                                | `UINT32`                   |
| `TIMESTAMP`                             | [DateTime64](/ar/sql-reference/data-types/datetime64.md)                                                            | `TIMESTAMP`                |
| `TIME32`, `TIME64`                      | [Time64](/ar/sql-reference/data-types/time64.md)                                                                    | `TIME32`, `TIME64`         |
| `STRING`, `BINARY`                      | [String](/ar/sql-reference/data-types/string.md)                                                                    | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                          | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/ar/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/ar/sql-reference/data-types/decimal.md)                                                               | `DECIMAL256`               |
| `LIST`                                  | [Array](/ar/sql-reference/data-types/array.md)                                                                      | `LIST`                     |
| `STRUCT`                                | [Tuple](/ar/sql-reference/data-types/tuple.md)                                                                      | `STRUCT`                   |
| `MAP`                                   | [Map](/ar/sql-reference/data-types/map.md)                                                                          | `MAP`                      |
| `UINT32`                                | [IPv4](/ar/sql-reference/data-types/ipv4.md)                                                                        | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/ar/sql-reference/data-types/ipv6.md)                                                                        | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/ar/sql-reference/data-types/int-uint.md)                                           | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/ar/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`                 |
| `INT64`                                 | [Interval](/ar/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year)   | `INT64`                    |

يمكن أن تكون أنواع `Array` متداخلة، كما يمكن أن تأخذ وسيطًا بقيمة من النوع `Nullable`. ويمكن أيضًا أن تكون الأنواع `Tuple` و`Map` متداخلة.

النوع `DICTIONARY` مدعوم في استعلامات `INSERT`، وبالنسبة إلى استعلامات `SELECT`، يوجد إعداد [`output_format_arrow_low_cardinality_as_dictionary`](/ar/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) يتيح إخراج النوع [LowCardinality](/ar/sql-reference/data-types/lowcardinality.md) بصيغة `DICTIONARY`. لاحظ أنه قد تكون هناك قيم غير مستخدمة في قاموس `LowCardinality`، مما قد يؤدي إلى ظهور قيم غير مستخدمة في `DICTIONARY` الخاص بـ Arrow عند الإخراج.

أنواع بيانات Arrow غير المدعومة:

* `FIXED_SIZE_BINARY`
* `JSON`
* `UUID`
* `ENUM`.

لا يلزم أن تتطابق أنواع بيانات أعمدة جدول ClickHouse مع حقول بيانات Arrow المناظرة. عند إدراج البيانات، يفسّر ClickHouse أنواع البيانات وفقًا للجدول أعلاه، ثم [يحوّل](/ar/sql-reference/functions/type-conversion-functions#CAST) البيانات إلى نوع البيانات المحدد لعمود جدول ClickHouse.

<div id="example-usage">
  ## مثال للاستخدام
</div>

في المثال أدناه، نستخدم مجموعة البيانات `forex` المتاحة في
[ساحة ClickHouse SQL التفاعلية](https://sql.clickhouse.com).

<div id="selecting-data">
  ### اختيار البيانات
</div>

نختار يومًا واحدًا من أسعار صرف `EUR/USD` من ساحة ClickHouse SQL التفاعلية ونحفظه
في ملف محلي باسم `forex_eurusd.arrow`. نجري استعلامًا على ساحة ClickHouse SQL التفاعلية عبر واجهة HTTP،
حيث يكون المضيف هو `sql-clickhouse.clickhouse.com` والمستخدم هو
`demo` (من دون كلمة مرور):

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### قراءة الملف مجددًا
</div>

يمكننا الآن قراءة ملف Arrow المحلي مجددًا باستخدام
[`clickhouse-local`](/ar/operations/utilities/clickhouse-local) عبر
دالة الجدول [`file`](/ar/sql-reference/table-functions/file). الملف
ذاتي الوصف، لذا يستنتج تنسيق `Arrow` المخطط تلقائيًا:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### إدراج البيانات
</div>

لتحميل ملف Arrow إلى جدول ClickHouse، مرّره إلى `clickhouse-client`
باستخدام `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                      | الوصف                                                                                           | الافتراضي   |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | السماح بالأعمدة المفقودة عند قراءة تنسيقات إدخال Arrow                                          | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | تجاهل حالة الأحرف عند مطابقة أعمدة Arrow مع أعمدة CH.                                           | `0`         |
| `input_format_arrow_import_nested`                                           | إعداد متقادم، لا تأثير له.                                                                      | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | تخطّي الأعمدة ذات الأنواع غير المدعومة أثناء استنتاج المخطط لتنسيق Arrow                        | `0`         |
| `output_format_arrow_compression_method`                                     | طريقة الضغط لتنسيق إخراج Arrow. خوارزميات الضغط المدعومة: lz4&#95;frame وzstd وnone (غير مضغوط) | `lz4_frame` |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | استخدم النوع Arrow FIXED&#95;SIZE&#95;BINARY بدلًا من Binary لأعمدة FixedString.                | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | تمكين إخراج النوع LowCardinality كنوع Arrow Dictionary                                          | `0`         |
| `output_format_arrow_string_as_string`                                       | استخدم النوع Arrow String بدلًا من Binary لأعمدة String                                         | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | استخدم دائمًا أعدادًا صحيحة من 64 بت لفهارس القاموس في تنسيق Arrow                              | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | استخدم أعدادًا صحيحة موقّعة لفهارس القاموس في تنسيق Arrow                                       | `1`         |