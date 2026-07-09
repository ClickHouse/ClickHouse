---
alias: []
description: 'توثيق صيغة HiveText'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✗       |                |

<div id="description">
  ## الوصف
</div>

يقرأ `HiveText` تنسيق التسلسل النصي المستخدم في جداول
[Apache Hive](https://hive.apache.org/)
(وهو التنسيق الذي ينتجه `LazySimpleSerDe` الخاص بـ Hive). وهو تنسيق نصي
مفصول بفواصل، يشبه [`CSV`](/ar/interfaces/formats/CSV)، حيث تُفصل الحقول
بفاصل Hive الافتراضي `\x01` ‏(Ctrl-A). ويمكن ضبط فاصل الحقول
عبر [`input_format_hive_text_fields_delimiter`](#format-settings).

`HiveText` هو تنسيق للإدخال فقط. ولا تحتوي البيانات على صف ترويسة: إذ تُطابَق القيم
موضعيًا مع أعمدة جدول الوجهة، لذلك تؤخذ أسماء الأعمدة وأنواعها من الجدول
(أو من بنية مُحددة صراحةً) بدلًا من استنتاجها من البيانات. وأثناء القراءة، يحلل ClickHouse
التواريخ والأوقات في وضع أفضل جهد (راجع [`date_time_input_format`](/ar/operations/settings/formats#date_time_input_format)),
ويملأ الحقول الختامية المحذوفة بالقيم الافتراضية للأعمدة، ويتجاوز الحقول التي لا
يتعرف عليها.

داخل الحقل، تُحلَّل القيم باستخدام قواعد الإفلات نفسها الخاصة بـ `CSV` بدلًا
من الفواصل المتداخلة الخاصة بـ Hive. وعلى وجه الخصوص، يُقرأ العمود من النوع
[`Array`](/ar/sql-reference/data-types/array) من
التمثيل الموضوع بين أقواس
(على سبيل المثال، `"['a','b','c']"`)، وليس من القيم المفصولة
بفاصل collection الخاص بـ Hive وهو `\x02`.

:::note لا تأثير لإعدادات الفواصل المتداخلة
يُقبل الإعدادان [`input_format_hive_text_collection_items_delimiter`](#format-settings) و
[`input_format_hive_text_map_keys_delimiter`](#format-settings)
لأغراض التوافق، لكن لا يُستخدمان حاليًا أثناء التحليل.
:::

بشكل افتراضي، يُسمح للصفوف بأن تحتوي على عدد متغير من الحقول (راجع
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)):
تُملأ الأعمدة المفقودة بالقيم الافتراضية في الصفوف التي تحتوي على حقول أقل من الجدول،
أما الصفوف التي تحتوي على حقول ختامية إضافية فيُتجاوز الزائد منها.

<div id="example-usage">
  ## مثال على الاستخدام
</div>

تستبدل الأمثلة أدناه فاصل الحقول الافتراضي بفاصلة (`,`) باستخدام
[`input_format_hive_text_fields_delimiter`](#format-settings)، بحيث تصبح ملفات
الإدخال سهلة القراءة.

<div id="reading-data">
  ### قراءة ملف HiveText
</div>

بافتراض وجود ملف `hive_data.txt` يحتوي على حقول مفصولة بفواصل:

```text title="hive_data.txt"
1,3
3,5,9
```

نُنشئ جدولًا يحدّد أسماء الأعمدة وأنواعها، ثم نُدرج الملف
فيه باستخدام `FORMAT HiveText`:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

لاحظ أن الصف الأول `1,3` يحتوي على حقلين فقط، لذلك يُملأ العمود المفقود `c`
بقيمة `0` الافتراضية.

<div id="variable-number-of-columns">
  ### عدد متغيّر من الأعمدة
</div>

عند استخدام الإعداد الافتراضي `input_format_hive_text_allow_variable_number_of_columns = 1`،
فإن الصفوف التي تحتوي على حقول أكثر مما يحتويه الجدول يتم فيها ببساطة
تجاهل الحقول الزائدة في النهاية:

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

بدلاً من ذلك، يفرض تعيين `input_format_hive_text_allow_variable_number_of_columns = 0`
عددًا صارمًا للحقول، ويؤدي وجود صفّ يحتوي على عدد حقول أقل من عدد أعمدة الجدول إلى حدوث
استثناء أثناء التحليل.

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| Setting                                                   | Description                                                                                                                       | Default |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `input_format_hive_text_fields_delimiter`                 | الفاصل بين الحقول في ملف Hive النصي                                                                                               | `\x01`  |
| `input_format_hive_text_collection_items_delimiter`       | الفاصل بين عناصر المجموعة (المصفوفة أو map) في ملف Hive النصي. يُقبل، لكنه غير مستخدم حاليًا عند التحليل.                         | `\x02`  |
| `input_format_hive_text_map_keys_delimiter`               | الفاصل بين كل زوج مفتاح/قيمة في map داخل ملف Hive النصي. يُقبل، لكنه غير مستخدم حاليًا عند التحليل.                               | `\x03`  |
| `input_format_hive_text_allow_variable_number_of_columns` | تجاهل الأعمدة الإضافية في مدخلات Hive Text (إذا كان الملف يحتوي على أعمدة أكثر من المتوقع) واعتبار الحقول المفقودة قيمًا افتراضية | `1`     |