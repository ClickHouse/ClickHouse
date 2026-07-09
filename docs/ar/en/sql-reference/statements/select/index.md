---
description: 'توثيق استعلام SELECT'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'استعلام SELECT'
doc_type: 'reference'
---

تُستخدم استعلامات `SELECT` لاسترجاع البيانات. وبشكل افتراضي، تُعاد البيانات المطلوبة إلى العميل، بينما يمكن، عند استخدامها مع [INSERT INTO](../../../sql-reference/statements/insert-into.md)، توجيهها إلى جدول آخر.

<div id="syntax">
  ## الصيغة
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

جميع البنود اختيارية، باستثناء قائمة التعبيرات المطلوبة التي تأتي مباشرةً بعد `SELECT`، والتي يرد شرحها بمزيد من التفصيل [أدناه](#select-clause).

تُشرح تفاصيل كل بند اختياري في أقسام منفصلة، وهي مُدرجة بالترتيب نفسه الذي تُنفَّذ به:

* [العبارة `WITH`](../../../sql-reference/statements/select/with.md)
* [العبارة `SELECT`](#select-clause)
* [العبارة `DISTINCT`](../../../sql-reference/statements/select/distinct.md)
* [العبارة `FROM`](../../../sql-reference/statements/select/from.md)
* [العبارة `SAMPLE`](../../../sql-reference/statements/select/sample.md)
* [العبارة `JOIN`](../../../sql-reference/statements/select/join.md)
* [العبارة `PREWHERE`](../../../sql-reference/statements/select/prewhere.md)
* [العبارة `WHERE`](../../../sql-reference/statements/select/where.md)
* [العبارة `WINDOW`](../../../sql-reference/window-functions/index.md)
* [بند GROUP BY](/ar/sql-reference/statements/select/group-by)
* [العبارة `LIMIT BY`](../../../sql-reference/statements/select/limit-by.md)
* [العبارة `HAVING`](../../../sql-reference/statements/select/having.md)
* [العبارة `QUALIFY`](../../../sql-reference/statements/select/qualify.md)
* [العبارة `LIMIT`](../../../sql-reference/statements/select/limit.md)
* [العبارة `OFFSET`](../../../sql-reference/statements/select/offset.md)
* [العبارة `UNION`](../../../sql-reference/statements/select/union.md)
* [العبارة `INTERSECT`](../../../sql-reference/statements/select/intersect.md)
* [العبارة `EXCEPT`](../../../sql-reference/statements/select/except.md)
* [العبارة `INTO OUTFILE`](../../../sql-reference/statements/select/into-outfile.md)
* [العبارة `FORMAT`](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## عبارة `SELECT`
</div>

تُحسَب [التعبيرات](/ar/sql-reference/syntax#expressions) المحددة في عبارة `SELECT` بعد اكتمال جميع العمليات في البنود الموضَّحة أعلاه. وتعمل هذه التعبيرات كما لو كانت تُطبَّق على صفوف منفصلة في النتيجة. وإذا كانت التعبيرات في عبارة `SELECT` تحتوي على دوال تجميع، فإن ClickHouse يعالج دوال التجميع والتعبيرات المستخدمة كوسيطات لها أثناء التجميع في [GROUP BY](/ar/sql-reference/statements/select/group-by).

إذا كنت تريد تضمين جميع الأعمدة في النتيجة، فاستخدم رمز النجمة (`*`). على سبيل المثال: `SELECT * FROM ...`.

<div id="dynamic-column-selection">
  ### التحديد الديناميكي للأعمدة
</div>

يتيح لك التحديد الديناميكي للأعمدة (المعروف أيضًا باسم تعبير COLUMNS) مطابقة بعض الأعمدة في النتيجة باستخدام تعبير نمطي من نوع [re2](https://en.wikipedia.org/wiki/RE2_\(software\)).

```sql
COLUMNS('regexp')
```

على سبيل المثال، تأمل الجدول التالي:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

يجلب الاستعلام التالي بيانات من جميع الأعمدة التي تحتوي أسماؤها على الرمز `a`.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

تُعاد الأعمدة المحددة بترتيب غير أبجدي.

يمكنك استخدام عدة تعبيرات `COLUMNS` في استعلام وتطبيق الدوال عليها.

على سبيل المثال:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

يُمرَّر كل عمود تُرجِعه العبارة `COLUMNS` إلى الدالة باعتباره وسيطًا منفصلًا. ويمكنك أيضًا تمرير وسائط أخرى إلى الدالة إذا كانت تدعمها. توخَّ الحذر عند استخدام الدوال. إذا كانت الدالة لا تدعم عدد الوسائط التي مرَّرتها إليها، يطرح ClickHouse استثناء.

على سبيل المثال:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

في هذا المثال، يُرجع `COLUMNS('a')` عمودين: `aa` و`ab`. ويُرجع `COLUMNS('c')` العمود `bc`. لا يمكن تطبيق العامل `+` على 3 وسائط، لذلك يطرح ClickHouse استثناءً مع الرسالة ذات الصلة.

يمكن أن تكون الأعمدة المطابقة للتعبير `COLUMNS` من أنواع بيانات مختلفة. إذا لم يطابق `COLUMNS` أي أعمدة وكان هو التعبير الوحيد في `SELECT`، فإن ClickHouse يطرح استثناءً.

<div id="select-columns-with-like-or-ilike">
  #### تحديد الأعمدة باستخدام `LIKE` أو `ILIKE`
</div>

يمكنك أيضًا تحديد الأعمدة عبر مطابقة أسمائها مع نمط يلي `*`، باستخدام `LIKE` المراعية لحالة الأحرف أو `ILIKE` غير المراعية لحالة الأحرف:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

تتبع أنماط `LIKE` و`ILIKE` قواعد `LIKE`، وليس قواعد التعبيرات النمطية. يطابق المحرف `%` أي سلسلة من المحارف، ويطابق المحرف `_` أي محرف واحد، ويُستخدم `\` لإفلات `%` و`_` و`\`. والفرق الوحيد بينهما هو أن `LIKE` يطابق أسماء الأعمدة مع مراعاة حالة الأحرف، بينما يكون `ILIKE` دون مراعاة حالة الأحرف. على سبيل المثال:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

يحدّد الاستعلام أعمدةً بأسماء مكوّنة من حرفين تبدأ بـ `a`، مثل `aa` و`ab`.

تدعم `* LIKE` و`* ILIKE` أيضًا النجوم المؤهلة ومحوّلات الأعمدة:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### النجمة
</div>

يمكنك وضع نجمة في أي جزء من الاستعلام بدلًا من تعبير. وعند تحليل الاستعلام، تُوسَّع النجمة إلى قائمة تضم جميع أعمدة الجدول (باستثناء الأعمدة `MATERIALIZED` و `ALIAS`). ولا يُبرَّر استخدام النجمة إلا في حالات قليلة:

* عند إنشاء dump لجدول.
* في الجداول التي لا تحتوي إلا على عدد قليل من الأعمدة، مثل جداول النظام.
* للحصول على معلومات حول الأعمدة الموجودة في جدول. في هذه الحالة، اضبط `LIMIT 1`. لكن من الأفضل استخدام الاستعلام `DESC TABLE`.
* عند وجود تصفية قوية على عدد صغير من الأعمدة باستخدام `PREWHERE`.
* في الاستعلامات الفرعية (لأن الأعمدة غير المطلوبة في الاستعلام الخارجي تُستبعَد من الاستعلامات الفرعية).

في جميع الحالات الأخرى، لا نوصي باستخدام النجمة، لأنها لا تمنحك إلا عيوب نظام إدارة قواعد بيانات قائم على الأعمدة بدلًا من مزاياه. وبعبارة أخرى، لا يُنصح باستخدام النجمة.

<div id="extreme-values">
  ### القيم القصوى
</div>

بالإضافة إلى النتائج، يمكنك أيضًا الحصول على القيم الصغرى والكبرى لأعمدة النتائج. وللقيام بذلك، اضبط الإعداد **extremes** على 1. تُحسَب القيم الصغرى والكبرى للأنواع الرقمية، والتواريخ، والتواريخ المقترنة بوقت. أما في الأعمدة الأخرى، فتُخرَج القيم الافتراضية.

يُحسَب صفّان إضافيان، هما صف القيم الصغرى وصف القيم الكبرى، على الترتيب. وتُخرَج هذان الصفّان الإضافيان في [التنسيقات](../../../interfaces/formats.md) `XML` و`JSON*` و`TabSeparated*` و`CSV*` و`Vertical` و`Template` و`Pretty*`، بشكل منفصل عن الصفوف الأخرى. ولا يُخرَجان في التنسيقات الأخرى.

في تنسيقات `JSON*` و`XML`، تُخرَج القيم القصوى في حقل منفصل باسم &#39;extremes&#39;. وفي تنسيقات `TabSeparated*` و`CSV*` و`Vertical`، يأتي الصف بعد النتيجة الرئيسية، وبعد &#39;totals&#39; إن وُجد. ويسبقه صف فارغ (بعد البيانات الأخرى). وفي تنسيقات `Pretty*`، يُخرَج الصف على هيئة جدول منفصل بعد النتيجة الرئيسية، وبعد `totals` إن وُجد. وفي تنسيق `Template`، تُخرَج القيم القصوى وفقًا للقالب المحدد.

تُحسَب القيم القصوى للصفوف قبل `LIMIT`، ولكن بعد `LIMIT BY`. ومع ذلك، عند استخدام `LIMIT offset, size`، تُدرَج الصفوف التي تسبق `offset` ضمن `extremes`. وفي الطلبات المتدفقة، قد تتضمن النتيجة أيضًا عددًا قليلًا من الصفوف التي تجاوزت `LIMIT`.

<div id="notes">
  ### ملاحظات
</div>

يمكنك استخدام الأسماء البديلة (الأسماء المستعارة `AS`) في أي جزء من الاستعلام.

يمكن أن تدعم بنود `GROUP BY` و`ORDER BY` و`LIMIT BY` الوسيطات الموضعية. لتمكين ذلك، فعِّل الإعداد [enable&#95;positional&#95;arguments](/ar/operations/settings/settings#enable_positional_arguments). بعد ذلك، على سبيل المثال، سيؤدي `ORDER BY 1,2` إلى فرز الصفوف في الجدول حسب العمود الأول ثم العمود الثاني.

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

إذا كان الاستعلام لا يتضمن عبارات `DISTINCT` و`GROUP BY` و`ORDER BY`، ولا الاستعلامات الفرعية `IN` و`JOIN`، فستتم معالجة الاستعلام بالكامل على نحو تدفقي باستخدام مقدار O(1) من RAM. بخلاف ذلك، قد يستهلك الاستعلام قدرًا كبيرًا من RAM إذا لم تُحدَّد القيود المناسبة:

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

لمزيد من المعلومات، راجع قسم &quot;الإعدادات&quot;. يمكن استخدام الفرز الخارجي (بحفظ الجداول المؤقتة على قرص) والتجميع الخارجي.

<div id="select-modifiers">
  ## مُعدِّلات SELECT
</div>

يمكنك استخدام المُعدِّلات التالية في استعلامات `SELECT`.

| Modifier                           | Description                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`APPLY`](./apply_modifier.md)     | يتيح لك استدعاء دالة معيّنة لكل صف تُعيده عبارة جدول خارجية في الاستعلام.                                                                                                                                                                                                                                                     |
| [`EXCEPT`](./except_modifier.md)   | يحدّد أسماء عمود واحد أو أكثر لاستبعادها من النتيجة. وتُحذف جميع أسماء الأعمدة المطابقة من المخرجات.                                                                                                                                                                                                                          |
| [`REPLACE`](./replace_modifier.md) | يحدّد [اسمًا مستعارًا لتعبير أو أكثر](/ar/sql-reference/syntax#expression-aliases). ويجب أن يطابق كل اسم مستعار اسمَ عمود في عبارة `SELECT *`. وفي قائمة أعمدة المخرجات، يُستبدل العمود الذي يطابق الاسم المستعار بالتعبير المحدَّد في `REPLACE`. ولا يغيّر هذا المُعدِّل أسماء الأعمدة أو ترتيبها، لكنه قد يغيّر القيمة ونوعها. |

<div id="modifier-combinations">
  ### تركيبات المُعدِّلات
</div>

يمكنك استخدام كل مُعدِّل على حدة أو دمجه مع غيره.

**أمثلة:**

استخدام المُعدِّل نفسه أكثر من مرة.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

استخدام عدة معدِّلات في استعلام واحد.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SETTINGS في استعلام SELECT
</div>

يمكنك تحديد الإعدادات المطلوبة مباشرةً ضمن استعلام `SELECT`. لا تُطبَّق قيمة الإعداد إلا على هذا الاستعلام، ثم تُعاد إلى القيمة الافتراضية أو السابقة بعد تنفيذ الاستعلام.

للتعرّف على طرق أخرى لضبط الإعدادات، راجع [هنا](/ar/operations/settings/overview).

بالنسبة إلى الإعدادات المنطقية المضبوطة على true، يمكنك استخدام صياغة مختصرة بحذف تعيين القيمة. وعند تحديد اسم الإعداد فقط، يُضبط تلقائيًا على `1` (true).

**مثال**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```