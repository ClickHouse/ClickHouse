---
description: 'توثيق لعبارة ORDER BY'
sidebar_label: 'ORDER BY'
slug: /sql-reference/statements/select/order-by
title: 'عبارة ORDER BY'
doc_type: 'مرجع'
---

تتضمن عبارة `ORDER BY` ما يلي:

* قائمة من التعبيرات، مثل `ORDER BY visits, search_phrase`،
* قائمة من الأرقام التي تشير إلى الأعمدة في عبارة `SELECT`، مثل `ORDER BY 2, 1`، أو
* `ALL`، وتعني جميع أعمدة عبارة `SELECT`، مثل `ORDER BY ALL`.

لإيقاف الفرز حسب أرقام الأعمدة، اضبط الإعداد [enable&#95;positional&#95;arguments](/ar/operations/settings/settings#enable_positional_arguments) = 0.
ولإيقاف الفرز حسب `ALL`، اضبط الإعداد [enable&#95;order&#95;by&#95;all](/ar/operations/settings/settings#enable_order_by_all) = 0.

يمكن أن تتضمن عبارة `ORDER BY` المُعدِّل `DESC` (تنازلي) أو `ASC` (تصاعدي) لتحديد اتجاه الفرز.
ما لم يُحدَّد ترتيب فرز صريح، يُستخدم `ASC` افتراضيًا.
وينطبق اتجاه الفرز على تعبير واحد، لا على القائمة بأكملها، مثل `ORDER BY Visits DESC, SearchPhrase`.
كذلك، يُجرى الفرز مع مراعاة حالة الأحرف.

تُعاد الصفوف التي تتطابق قيمها في تعبيرات الفرز بترتيب عشوائي وغير حتمي.
وإذا حُذفت عبارة `ORDER BY` من تعليمة `SELECT`، فإن ترتيب الصفوف يكون أيضًا عشوائيًا وغير حتمي.

<div id="sorting-of-special-values">
  ## ترتيب القيم الخاصة
</div>

هناك طريقتان لترتيب فرز `NaN` و`NULL`:

* افتراضيًا أو مع المُعدِّل `NULLS LAST`: تأتي القيم أولًا، ثم `NaN`، ثم `NULL`.
* مع المُعدِّل `NULLS FIRST`: يأتي `NULL` أولًا، ثم `NaN`، ثم القيم الأخرى.

<div id="example">
  ### مثال
</div>

بالنسبة إلى الجدول

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

نفِّذ الاستعلام `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` للحصول على:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

عند فرز أعداد الفاصلة العائمة، تكون قيم NaN منفصلة عن القيم الأخرى. وبغضّ النظر عن ترتيب الفرز، تأتي قيم NaN في النهاية. وبعبارة أخرى، عند الفرز التصاعدي تُوضَع كما لو كانت أكبر من جميع الأعداد الأخرى، بينما عند الفرز التنازلي تُوضَع كما لو كانت أصغر من سائر القيم.

<div id="collation-support">
  ## دعم Collation
</div>

لفرز قيم [String](../../../sql-reference/data-types/string.md)، يمكنك تحديد collation (المقارنة). مثال: `ORDER BY SearchPhrase COLLATE 'tr'` - لفرز الكلمات المفتاحية بترتيب تصاعدي باستخدام الأبجدية التركية، مع تجاهل حالة الأحرف، على افتراض أن السلاسل النصية مرمّزة بترميز UTF-8. يمكن تحديد `COLLATE` أو عدم تحديده لكل تعبير في ORDER BY على نحو مستقل. وإذا تم تحديد `ASC` أو `DESC`، فسيُحدَّد `COLLATE` بعده. عند استخدام `COLLATE`، يكون الفرز دائمًا غير حساس لحالة الأحرف.

‏يكون Collate مدعومًا في [LowCardinality](../../../sql-reference/data-types/lowcardinality.md)، و[Nullable](../../../sql-reference/data-types/nullable.md)، و[Array](../../../sql-reference/data-types/array.md)، و[Tuple](../../../sql-reference/data-types/tuple.md).

نوصي باستخدام `COLLATE` فقط للفرز النهائي لعدد صغير من الصفوف، لأن الفرز باستخدام `COLLATE` أقل كفاءة من الفرز العادي بحسب البايتات.

<div id="collation-examples">
  ## أمثلة على Collation
</div>

مثال يقتصر على قيم [String](../../../sql-reference/data-types/string.md) فقط:

جدول الإدخال:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

مثال مع [Nullable](../../../sql-reference/data-types/nullable.md):

جدول الإدخال:

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

مثال باستخدام [Array](../../../sql-reference/data-types/array.md):

جدول الإدخال:

```text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

مثال على سلسلة نصية من نوع [LowCardinality](../../../sql-reference/data-types/lowcardinality.md):

جدول الإدخال:

```response
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

مثال على استخدام [Tuple](../../../sql-reference/data-types/tuple.md):

```response title="Response"
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

يُستخدم قدر أقل من RAM إذا جرى تحديد [LIMIT](../../../sql-reference/statements/select/limit.md) صغير بما يكفي إلى جانب `ORDER BY`. وبخلاف ذلك، تكون كمية الذاكرة المستخدمة متناسبة مع حجم البيانات المراد فرزها. وفي حالة معالجة الاستعلامات الموزعة، إذا تم حذف [GROUP BY](/ar/sql-reference/statements/select/group-by)، يُنفَّذ الفرز جزئيًا على الخوادم البعيدة، ثم تُدمَج النتائج على الخادم الذي أرسل الطلب. وهذا يعني أنه في الفرز الموزع قد يكون حجم البيانات المطلوب فرزها أكبر من مقدار الذاكرة المتاح على خادم واحد.

إذا لم تكن هناك ذاكرة RAM كافية، فمن الممكن إجراء الفرز باستخدام الذاكرة الخارجية (من خلال إنشاء ملفات مؤقتة على القرص). استخدم الإعداد `max_bytes_before_external_sort` لهذا الغرض. إذا كانت قيمته 0 (وهي القيمة الافتراضية)، فسيتم تعطيل الفرز الخارجي. وإذا كان مفعّلًا، فعندما يصل حجم البيانات المطلوب فرزها إلى عدد البايتات المحدد، تُفرَز البيانات المجمَّعة وتُحفَظ في ملف مؤقت. وبعد قراءة جميع البيانات، تُدمَج كل الملفات المفروزة وتُخرَج النتائج. تُكتَب الملفات إلى الدليل ‏`/var/lib/clickhouse/tmp/` في التهيئة (افتراضيًا، ولكن يمكنك استخدام المعلَمة ‏`tmp_path` لتغيير هذا الإعداد). ويمكنك أيضًا استخدام النقل إلى القرص فقط إذا تجاوز الاستعلام حدود الذاكرة؛ أي إن `max_bytes_ratio_before_external_sort=0.6` سيفعّل النقل إلى القرص فقط عندما يصل الاستعلام إلى حد الذاكرة `60%` ‏(المستخدم/الخادم).

قد يستهلك تشغيل استعلام ذاكرة أكبر من `max_bytes_before_external_sort`. ولهذا السبب، يجب أن تكون قيمة هذا الإعداد أصغر بكثير من `max_memory_usage`. على سبيل المثال، إذا كان لدى خادمك 128 GB من RAM وكنت تحتاج إلى تشغيل استعلام واحد، فاضبط `max_memory_usage` على 100 GB، و`max_bytes_before_external_sort` على 80 GB.

يعمل الفرز الخارجي بكفاءة أقل بكثير من الفرز داخل RAM.

<div id="optimization-of-data-reading">
  ## تحسين قراءة البيانات
</div>

إذا كان تعبير `ORDER BY` يتضمن بادئة تتطابق مع مفتاح الفرز للجدول، فيمكنك تحسين الاستعلام باستخدام الإعداد [optimize&#95;read&#95;in&#95;order](../../../operations/settings/settings.md#optimize_read_in_order).

عند تمكين الإعداد `optimize_read_in_order`، يستخدم ClickHouse server فهرس الجدول ويقرأ البيانات وفق ترتيب مفتاح `ORDER BY`. ويتيح ذلك تجنب قراءة جميع البيانات عند تحديد [LIMIT](../../../sql-reference/statements/select/limit.md). لذلك تُعالَج الاستعلامات على البيانات الكبيرة ذات `LIMIT` الصغير بسرعة أكبر.

يعمل هذا التحسين مع كلٍّ من `ASC` و`DESC`، ولا يعمل مع عبارة [GROUP BY](/ar/sql-reference/statements/select/group-by) والمُعدِّل [FINAL](/ar/sql-reference/statements/select/from#final-modifier).

عند تعطيل الإعداد `optimize_read_in_order`، لا يستخدم ClickHouse server فهرس الجدول أثناء معالجة استعلامات `SELECT`.

فكّر في تعطيل `optimize_read_in_order` يدويًا عند تشغيل استعلامات تتضمن عبارة `ORDER BY` و`LIMIT` كبيرًا وشرط [WHERE](../../../sql-reference/statements/select/where.md) يتطلب قراءة عدد هائل من السجلات قبل العثور على البيانات المطلوبة.

هذا التحسين مدعوم في محركات الجداول التالية:

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك [materialized views](/ar/sql-reference/statements/create/view#materialized-view)),
* [Merge](../../../engines/table-engines/special/merge.md),
* [Buffer](../../../engines/table-engines/special/buffer.md)

في الجداول التي تستخدم المحرك `MaterializedView`، يعمل هذا التحسين مع العروض مثل `SELECT ... FROM merge_tree_table ORDER BY pk`. لكنه غير مدعوم في استعلامات مثل `SELECT ... FROM view ORDER BY pk` إذا كان استعلام العرض لا يحتوي على عبارة `ORDER BY`.

<div id="order-by-expr-with-fill-modifier">
  ## مُعدِّل ORDER BY Expr WITH FILL
</div>

يمكن أيضًا استخدام هذا المُعدِّل مع [مُعدِّل LIMIT ... WITH TIES](/ar/sql-reference/statements/select/limit#limit--with-ties-modifier).

يمكن وضع المُعدِّل `WITH FILL` بعد `ORDER BY expr` مع المَعلَمات الاختيارية `FROM expr` و`TO expr` و`STEP expr`.
ستُستكمَل جميع القيم المفقودة في العمود `expr` بالتسلسل، وستُملأ الأعمدة الأخرى بالقيم الافتراضية.

لملء عدة أعمدة، أضف المُعدِّل `WITH FILL` مع المَعلَمات الاختيارية بعد اسم كل حقل في قسم `ORDER BY`.

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

يمكن تطبيق `WITH FILL` على الحقول ذات الأنواع Numeric ‏(جميع أنواع float وdecimal وint) أو الأنواع Date/DateTime. وعند تطبيقه على حقول `String`، تُملأ القيم المفقودة بسلاسل فارغة.
عندما لا يكون `FROM const_expr` معرّفًا، يستخدم تسلسل الملء القيمة الدنيا للحقل `expr` من `ORDER BY`.
عندما لا يكون `TO const_expr` معرّفًا، يستخدم تسلسل الملء القيمة القصوى للحقل `expr` من `ORDER BY`.
عندما يكون `STEP const_numeric_expr` معرّفًا، يُفسَّر `const_numeric_expr` `as is` للأنواع الرقمية، و`days` لنوع Date، و`seconds` لنوع DateTime. كما يدعم نوع البيانات [INTERVAL](/ar/sql-reference/data-types/special-data-types/interval/) الذي يمثّل فواصل زمنية للتاريخ والوقت.
عندما يُحذف `STEP const_numeric_expr`، يستخدم تسلسل الملء `1.0` للنوع الرقمي، و`1 day` لنوع Date، و`1 second` لنوع DateTime.
عندما يكون `STALENESS const_numeric_expr` معرّفًا، سينشئ الاستعلام صفوفًا إلى أن يتجاوز الفرق عن الصف السابق في البيانات الأصلية القيمة `const_numeric_expr`.
يمكن تطبيق `INTERPOLATE` على الأعمدة التي لا تشارك في `ORDER BY WITH FILL`. وتُملأ هذه الأعمدة استنادًا إلى قيم الحقول السابقة بتطبيق `expr`. وإذا لم يكن `expr` موجودًا، فستُكرَّر القيمة السابقة. ويؤدي حذف القائمة إلى تضمين جميع الأعمدة المسموح بها.

مثال على استعلام بدون `WITH FILL`:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

الاستعلام نفسه بعد تطبيق المُعدِّل `WITH FILL`:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

في الحالة التي تتضمن عدة حقول `ORDER BY field2 WITH FILL, field1 WITH FILL`، سيكون ترتيب الملء وفقًا لترتيب الحقول في عبارة `ORDER BY`.

مثال:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

الحقل `d1` لا يُملأ ولا يستخدم القيمة الافتراضية، لأنّه لا توجد لدينا قيم متكررة للحقل `d2`، ولا يمكن حساب التسلسل الخاص بـ `d1` بشكل صحيح.

الاستعلام التالي مع تغيير الحقل في `ORDER BY`:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

يستخدم الاستعلام التالي نوع البيانات `INTERVAL` بقيمة يوم واحد لكل قيمة يتم ملؤها في العمود `d1`:

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

```response title="Response"
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

مثال على استعلام من دون `STALENESS`:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

الاستعلام نفسه بعد تطبيق `STALENESS 3`:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

مثال على استعلام بدون `INTERPOLATE`:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

نفس الاستعلام بعد استخدام `INTERPOLATE`:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

<div id="filling-grouped-by-sorting-prefix">
  ## الملء مع التجميع حسب بادئة الفرز
</div>

قد يكون من المفيد ملء الصفوف التي تتطابق فيها قيم أعمدة معيّنة، كلٌّ على حدة. ومن الأمثلة الجيدة على ذلك ملء القيم المفقودة في السلاسل الزمنية.
لنفترض وجود جدول السلاسل الزمنية التالي:

```sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

ونرغب في ملء القيم المفقودة لكل مستشعر بشكل مستقل، بفاصل زمني مقداره ثانية واحدة.
ولتحقيق ذلك، استخدم العمود `sensor_id` كبادئة فرز لملء العمود `timestamp`:

```sql
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

هنا، استُخدمت القيمة `9999` في استيفاء العمود `value` فقط لجعل الصفوف المُعبأة أكثر وضوحًا.
يُتحكَّم في هذا السلوك من خلال الإعداد `use_with_fill_by_sorting_prefix` (مُمكَّن افتراضيًا)

<div id="related-content">
  ## محتوى مرتبط
</div>

* مدونة: [العمل مع بيانات السلاسل الزمنية في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)