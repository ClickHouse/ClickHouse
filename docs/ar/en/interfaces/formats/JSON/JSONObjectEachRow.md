---
alias: []
description: 'توثيق تنسيق JSONObjectEachRow'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

في هذه الصيغة، تُمثَّل جميع البيانات على شكل كائن JSON واحد، ويُمثَّل كل صف كحقل منفصل داخل هذا الكائن، على نحو مشابه لتنسيق [`JSONEachRow`](./JSONEachRow.md).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="basic-example">
  ### مثال أساسي
</div>

لنفترض وجود JSON كالتالي:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

لاستخدام اسم كائن كقيمة لعمود، يمكنك استخدام الإعداد الخاص [`format_json_object_each_row_column_for_object_name`](/ar/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name).
تُعيَّن قيمة هذا الإعداد إلى اسم عمود، ويُستخدم هذا العمود كمفتاح JSON للصف في الكائن الناتج.

<div id="output">
  #### المخرجات
</div>

لنفترض أن لدينا الجدول `test` بعمودين:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

لنعرِض الناتج بتنسيق `JSONObjectEachRow` ونستخدم الإعداد `format_json_object_each_row_column_for_object_name`:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### الإدخال
</div>

لنفترض أننا حفظنا المخرجات من المثال السابق في ملف باسم `data.json`:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

ويعمل أيضًا مع استنتاج المخطط:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### إدراج البيانات
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

يسمح ClickHouse بما يلي:

* أي ترتيب لأزواج المفتاح والقيمة داخل الكائن.
* حذف بعض القيم.

يتجاهل ClickHouse المسافات بين العناصر والفواصل التي تأتي بعد الكائنات. يمكنك تمرير جميع الكائنات في سطر واحد. ولا يلزم فصلها بأسطر جديدة.

<div id="omitted-values-processing">
  #### معالجة القيم المتروكة
</div>

يستبدل ClickHouse القيم المتروكة بالقيم الافتراضية [لأنواع البيانات](/ar/sql-reference/data-types/index.md) المقابلة.

إذا كان `DEFAULT expr` محددًا، فإن ClickHouse يستخدم قواعد استبدال مختلفة اعتمادًا على الإعداد [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields).

انظر إلى الجدول التالي:

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* إذا كانت `input_format_defaults_for_omitted_fields = 0`، فإن القيمة الافتراضية لكلٍّ من `x` و`a` هي `0` (وهي القيمة الافتراضية لنوع البيانات `UInt32`).
* إذا كانت `input_format_defaults_for_omitted_fields = 1`، فإن القيمة الافتراضية لـ `x` هي `0`، لكن القيمة الافتراضية لـ `a` هي `x * 2`.

:::note
عند إدراج البيانات باستخدام `input_format_defaults_for_omitted_fields = 1`، يستهلك ClickHouse موارد حوسبة أكبر مقارنةً بإدراجها باستخدام `input_format_defaults_for_omitted_fields = 0`.
:::

<div id="json-selecting-data">
  ### استعلام البيانات
</div>

خذ جدول `UserActivity` على سبيل المثال:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

يعيد الاستعلام `SELECT * FROM UserActivity FORMAT JSONEachRow` ما يلي:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

على عكس تنسيق [JSON](/ar/interfaces/formats/JSON)، لا تُستبدل تسلسلات UTF-8 غير الصالحة. وتُطبَّق على القيم عملية الإفلات بالطريقة نفسها المستخدمة في `JSON`.

:::info
يمكن إخراج أي مجموعة من البايتات ضمن السلاسل النصية. استخدم تنسيق [`JSONEachRow`](./JSONEachRow.md) إذا كنت متأكدًا من أن البيانات في الجدول يمكن تنسيقها بصيغة JSON دون فقدان أي معلومات.
:::

<div id="jsoneachrow-nested">
  ### استخدام البُنى Nested
</div>

إذا كان لديك جدول يحتوي على أعمدة من نوع البيانات [`Nested`](/ar/sql-reference/data-types/nested-data-structures/index.md)، فيمكنك إدراج بيانات JSON بالبنية ذاتها. فعِّل هذه الميزة من خلال الإعداد [input&#95;format&#95;import&#95;nested&#95;json](/ar/operations/settings/settings-formats.md/#input_format_import_nested_json).

على سبيل المثال، تأمّل الجدول التالي:

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

كما ترى في وصف نوع البيانات `Nested`، يتعامل ClickHouse مع كل مكوّن من مكوّنات البنية المتداخلة على أنه عمود مستقل (`n.s` و `n.i` في جدولنا). يمكنك إدراج البيانات بالطريقة التالية:

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

لإدراج البيانات على شكل كائن JSON هرمي، اضبط [`input_format_import_nested_json=1`](/ar/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

من دون هذا الإعداد، يطرح ClickHouse استثناءً.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## إعدادات الصيغة
</div>

| الإعداد                                                                                                                                                                      | الوصف                                                                                                                                                            | القيمة الافتراضية | ملاحظات                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`input_format_import_nested_json`](/ar/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | ربط بيانات JSON المتداخلة بالجداول المتداخلة (يعمل هذا مع format‏ JSONEachRow).                                                                                  | `false`           |                                                                                                                                                                                             |
| [`input_format_json_read_bools_as_numbers`](/ar/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | السماح بتحليل القيم المنطقية كأرقام في JSON input formats.                                                                                                       | `true`            |                                                                                                                                                                                             |
| [`input_format_json_read_bools_as_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | يسمح بتحليل القيم المنطقية كسلاسل نصية في تنسيقات إدخال JSON.                                                                                                    | `true`            |                                                                                                                                                                                             |
| [`input_format_json_read_numbers_as_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | يسمح بتحليل الأرقام كسلاسل نصية في تنسيقات إدخال JSON.                                                                                                           | `true`            |                                                                                                                                                                                             |
| [`input_format_json_read_arrays_as_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | يسمح بتحليل مصفوفات JSON كسلاسل نصية في تنسيقات إدخال JSON.                                                                                                      | `true`            |                                                                                                                                                                                             |
| [`input_format_json_read_objects_as_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | السماح بتحليل كائنات JSON كسلاسل نصية في تنسيقات إدخال JSON.                                                                                                     | `true`            |                                                                                                                                                                                             |
| [`input_format_json_named_tuples_as_objects`](/ar/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | تحليل أعمدة الـnamed tuple ككائنات JSON.                                                                                                                         | `true`            |                                                                                                                                                                                             |
| [`input_format_json_try_infer_numbers_from_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | محاولة استنتاج الأرقام من الحقول النصية أثناء استنتاج المخطط.                                                                                                    | `false`           |                                                                                                                                                                                             |
| [`input_format_json_try_infer_named_tuples_from_objects`](/ar/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | حاوِل استنتاج named tuple من JSON objects أثناء استنتاج المخطط.                                                                                                  | `true`            |                                                                                                                                                                                             |
| [`input_format_json_infer_incomplete_types_as_strings`](/ar/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | استخدم النوع String للمفاتيح التي تحتوي فقط على قيم NULL أو JSON objects/arrays فارغة أثناء استنتاج المخطط في JSON input formats.                                | `true`            |                                                                                                                                                                                             |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/ar/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | أدرِج default values للعناصر المفقودة في JSON object أثناء تحليل named tuple.                                                                                    | `true`            |                                                                                                                                                                                             |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/ar/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | تجاهل المفاتيح غير المعروفة في كائن JSON داخل Tuples المُسمّاة.                                                                                                  | `false`           |                                                                                                                                                                                             |
| [`input_format_json_compact_allow_variable_number_of_columns`](/ar/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | السماح بعدد متغيّر من الأعمدة في تنسيق JSONCompact/JSONCompactEachRow، مع تجاهل الأعمدة الإضافية واستخدام القيم الافتراضية للأعمدة المفقودة.                     | `false`           |                                                                                                                                                                                             |
| [`input_format_json_throw_on_bad_escape_sequence`](/ar/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | إصدار استثناء إذا كانت سلسلة JSON تحتوي على تسلسل هروب غير صالح. وإذا كان هذا الخيار معطّلًا، فستبقى تسلسلات الهروب غير الصالحة كما هي في البيانات.              | `true`            |                                                                                                                                                                                             |
| [`input_format_json_empty_as_default`](/ar/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | يعامل الحقول الفارغة في مدخلات JSON على أنها قيم افتراضية.                                                                                                       | `false`.          | بالنسبة إلى التعبيرات الافتراضية المعقدة، يجب أيضًا تفعيل [`input_format_defaults_for_omitted_fields`](/ar/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [`output_format_json_quote_64bit_integers`](/ar/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | يتحكم في وضع علامات اقتباس حول الأعداد الصحيحة 64-بت في تنسيق إخراج JSON.                                                                                        | `true`            |                                                                                                                                                                                             |
| [`output_format_json_quote_64bit_floats`](/ar/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | يتحكم في وضع علامات اقتباس حول الأعداد ذات الفاصلة العائمة 64-بت في تنسيق إخراج JSON.                                                                            | `false`           |                                                                                                                                                                                             |
| [`output_format_json_quote_denormals`](/ar/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | يتيح إخراج &#39;+nan&#39; و&#39;-nan&#39; و&#39;+inf&#39; و&#39;-inf&#39; في تنسيق إخراج JSON.                                                                   | `false`           |                                                                                                                                                                                             |
| [`output_format_json_quote_decimals`](/ar/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | يتحكم في وضع القيم العشرية بين علامتَي اقتباس في تنسيق إخراج JSON.                                                                                               | `false`           |                                                                                                                                                                                             |
| [`output_format_json_escape_forward_slashes`](/ar/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | يتحكم في إفلات الشرطات المائلة للأمام في مخرجات السلاسل النصية ضمن تنسيق إخراج JSON.                                                                             | `true`            |                                                                                                                                                                                             |
| [`output_format_json_named_tuples_as_objects`](/ar/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | يُسلسِل أعمدة named tuple على هيئة JSON objects.                                                                                                                 | `true`            |                                                                                                                                                                                             |
| [`output_format_json_array_of_rows`](/ar/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | يُخرج JSON array يضم جميع الصفوف بتنسيق JSONEachRow(Compact).                                                                                                    | `false`           |                                                                                                                                                                                             |
| [`output_format_json_validate_utf8`](/ar/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | يُمكّن التحقق من تسلسلات UTF-8 في تنسيقات إخراج JSON (لاحظ أن ذلك لا يؤثر في التنسيقات JSON/JSONCompact/JSONColumnsWithMetadata، إذ إنها تتحقق دائمًا من UTF-8). | `false`           |                                                                                                                                                                                             |