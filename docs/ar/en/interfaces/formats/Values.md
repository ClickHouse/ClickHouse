---
alias: []
description: 'توثيق تنسيق Values'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يعرض تنسيق `Values` كل صف داخل أقواس.

* تُفصل الصفوف بفواصل، من دون فاصلة بعد الصف الأخير.
* تُفصل القيم داخل الأقواس أيضًا بفواصل.
* تُخرج الأرقام بتنسيق عشري من دون علامات اقتباس.
* تُخرج المصفوفات ضمن `[]`.
* تُخرج السلاسل النصية والتواريخ والتواريخ مع الوقت بين علامتَي اقتباس.
* قواعد الإفلات والتحليل مماثلة لتنسيق [TabSeparated](TabSeparated/TabSeparated.md).

أثناء التنسيق، لا تُضاف مسافات إضافية، ولكن أثناء التحليل يُسمح بها ويجري تجاهلها (باستثناء المسافات داخل قيم المصفوفة، فهي غير مسموح بها).
يُمثَّل [`NULL`](/ar/sql-reference/syntax.md) على أنه `NULL`.

الحد الأدنى من الأحرف التي تحتاج إلى إفلاتها عند تمرير البيانات بتنسيق `Values`:

* علامات الاقتباس المفردة
* الشرطات المائلة العكسية

هذا هو التنسيق المستخدم في `INSERT INTO t VALUES ...`، ولكن يمكنك أيضًا استخدامه لتنسيق نتائج الاستعلام.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

تنسيق `Values` هو التنسيق الذي تستخدمه `INSERT`، لذا فإن أي تعليمة `INSERT ... VALUES`
تستخدمه بالفعل. ويمكن ذكر بند `FORMAT Values` صراحةً، كما يمكن
توفير الصفوف من تدفق أو ملف. ويمثل كل صف tuple بين قوسين،
تُفصل عناصره بفواصل، كما تُفصل الـ tuples نفسها بفواصل:

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### استخدام التعبيرات في الإدخال
</div>

على عكس معظم تنسيقات الإدخال، يمكن لـ `Values` تقييم تعبيرات SQL في كل حقل،
بدلًا من الاقتصار على قبول القيم الحرفية فقط. يتحكم في ذلك
[`input_format_values_interpret_expressions`](#format-settings) (مفعّل
افتراضيًا): فعندما يتعذر قراءة حقل بواسطة محلل التدفق السريع، يعود ClickHouse
إلى محلل SQL ويفسّر الحقل على أنه تعبير.

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### تحديد البيانات
</div>

يمكن أيضًا استخدام تنسيق `Values` لتنسيق نتائج الاستعلام. تُكتب الأرقام
من دون علامات اقتباس، والمصفوفات داخل `[]`، والسلاسل النصية والتواريخ بين علامتَي اقتباس مفردتين؛
وتُسبق علامات الاقتباس المفردة والشرطة المائلة العكسية داخل السلاسل النصية بشرطة مائلة عكسية، كما
تُكتب [`NULL`](/ar/sql-reference/syntax.md) على هيئة `NULL`:

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                                                     | الوصف                                                                                                                                                          | الافتراضي |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | إذا تعذّر على محلل التدفق تحليل الحقل، فشغّل محلل SQL وحاول تفسيره على أنه تعبير SQL.                                                                          | `true`    |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | إذا تعذّر على محلل التدفق تحليل الحقل، فشغّل محلل SQL، واستنتج قالب تعبير SQL، ثم حاول تحليل جميع الصفوف باستخدام القالب وبعد ذلك فسّر التعبير في جميع الصفوف. | `true`    |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | عند تحليل التعبيرات وتفسيرها باستخدام القالب، تحقّق من النوع الفعلي للقيمة الحرفية لتجنّب مشكلات تجاوز السعة والدقة المحتملة.                                  | `true`    |