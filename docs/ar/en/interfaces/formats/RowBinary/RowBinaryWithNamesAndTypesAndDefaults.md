---
alias: []
description: 'وثائق تنسيق RowBinaryWithNamesAndTypesAndDefaults'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| المدخلات | المخرجات | اسم بديل |
| -------- | -------- | -------- |
| ✔        | ✗        |          |

<div id="description">
  ## الوصف
</div>

يشبه تنسيق [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md)، لكنه يضيف بايتًا إضافيًا قبل كل خلية يحدد ما إذا كان ينبغي استخدام القيمة `DEFAULT` الخاصة بالعمود — تمامًا كما في تنسيق [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md). يتيح هذا الدمج تنفيذ عمليات `INSERT` مع تطور المخطط: إذ يمكن للجهة الكاتبة حذف أعمدة من الترويسة (فتأخذ القيمة `DEFAULT` الخاصة بالعمود الهدف)، وبالنسبة إلى أي عمود ترسله، يمكنها أيضًا تعليم خلايا مفردة على أنها &quot;استخدم القيمة `DEFAULT` الخاصة بالعمود&quot; من دون الخلط بين ذلك وبين `NULL`.

هذا التنسيق مخصص للإدخال فقط.

<div id="wire-format">
  ## تنسيق النقل
</div>

الترويسة مطابقة لـ [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md):

1. قيمة `VarUInt` تمثل عدد الأعمدة `N`.
2. `N` من سلاسل `String` المسبوقة بالطول، وتحمل أسماء الأعمدة.
3. `N` من أنواع الأعمدة — إما أسماء نصية أو ترميزًا ثنائيًا مضغوطًا، وتتحكم فيه الإعدادات `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format`.

بعد الترويسة، يتكوّن كل صف من `N` خلية. ولكل خلية:

* بايت وسم واحد من نوع `UInt8`.
  * `0x01` — استخدم تعبير `DEFAULT` للعمود الهدف. لا تتبعه أي بايتات للقيمة.
  * `0x00` — تتبع ذلك قيمة، وتُسلسَل باستخدام مُسلسِل `RowBinary` الخاص بنوع العمود. بالنسبة إلى `Nullable(T)`، تبدأ بايتات القيمة ببايت NULL الخاص بـ `Nullable` (`0` للقيمة غير NULL، و`1` لـ NULL)، ثم القيمة الداخلية إذا لم تكن NULL.

<div id="defaults-vs-null">
  ## القيم الافتراضية مقابل NULL
</div>

الوسم الافتراضي على مستوى كل خلية وبايت null المضمّن في `Nullable` مستقلان عن بعضهما. ويمكن إرسال العمود `Nullable(UInt32) DEFAULT 42` بثلاث طرق مختلفة لكل صف:

| البايتات  | المعنى                                       |
| --------- | -------------------------------------------- |
| `01`      | استخدم `DEFAULT 42`.                         |
| `00 01`   | مسار القيمة، ثم `NULL` عبر النوع `Nullable`. |
| `00 00 …` | مسار القيمة، ثم قيمة داخلية غير NULL.        |

<div id="schema-evolution">
  ## تطوّر المخطط
</div>

| الحالة                                               | السلوك                                                                                                                                                                 |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| عمود مفقود بالكامل من ترويسة الملف                   | يُملأ في الهدف عبر `insertDefaultsForNotSeenColumns`؛ ويتحكم فيه `defaults_for_omitted_fields`.                                                                        |
| عمود موجود في الترويسة، وسم الخلية `0x01`            | `insertDefault` لكل صف.                                                                                                                                                |
| عمود موجود في الترويسة، وسم الخلية `0x00`            | تُحلَّل القيمة كالمعتاد.                                                                                                                                               |
| عمود إضافي في الترويسة، غير موجود في الجدول المستهدف | يُتجاهَل بصمت عند `input_format_skip_unknown_fields = 1` (يُستهلك الوسم أولًا؛ إذا كان `0x01` فلا شيء آخر، وإذا كان `0x00` فتُحلَّل القيمة الموحَّدة النوع ثم تُهمَل). |

<div id="example-usage">
  ## مثال للاستخدام
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* تتضمن الترويسة عمودًا واحدًا باسم `x` من النوع `Nullable(UInt32)`.
* تستخدم الخلية الوحيدة الوسم `0x01`، ما يعني &quot;استخدم `DEFAULT 42`&quot;.

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<RowBinaryFormatSettings />