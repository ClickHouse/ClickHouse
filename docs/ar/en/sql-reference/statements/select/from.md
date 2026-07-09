---
description: 'توثيق لعبارة FROM'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'عبارة FROM'
doc_type: 'reference'
---

تُحدِّد عبارة `FROM` المصدر الذي تُقرأ منه البيانات:

* [الجدول](../../../engines/table-engines/index.md)
* [الاستعلام الفرعي](../../../sql-reference/statements/select/index.md)
* [دالة الجدول](/ar/sql-reference/table-functions)

يمكن أيضًا استخدام عبارتي [JOIN](../../../sql-reference/statements/select/join.md) و[ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) لتوسيع إمكانات عبارة `FROM`.

الاستعلام الفرعي هو استعلام `SELECT` آخر يمكن تحديده بين قوسين داخل عبارة `FROM`.

يمكن أيضًا استخدام عبارة `VALUES` القياسية في SQL كتعبير جدولي:

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

راجع [دالة الجدول Values](/ar/sql-reference/table-functions/values#sql-standard-values-clause) لمزيد من التفاصيل.

يمكن أن يحتوي `FROM` على عدة مصادر بيانات تفصل بينها فواصل، وهو ما يعادل إجراء [CROSS JOIN](../../../sql-reference/statements/select/join.md) عليها.

يمكن أن يظهر `FROM` اختياريًا قبل عبارة `SELECT`. هذا امتداد خاص بـ ClickHouse إلى SQL القياسي، ما يجعل عبارات `SELECT` أسهل في القراءة. مثال:

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## مُعدِّل FINAL
</div>

عند تحديد `FINAL`، يدمج ClickHouse البيانات بالكامل قبل إرجاع النتيجة. ويؤدي ذلك أيضًا إلى تنفيذ جميع تحوّلات البيانات التي تحدث أثناء عمليات الدمج لمحرك الجدول المحدد.

وينطبق ذلك عند اختيار البيانات من الجداول التي تستخدم محركات الجداول التالية:

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

تُنفَّذ استعلامات `SELECT` التي تستخدم `FINAL` بالتوازي. ويحدّ الإعداد [max&#95;final&#95;threads](/ar/operations/settings/settings#max_final_threads) من عدد مؤشرات الترابط المستخدمة.

<div id="drawbacks">
  ### العيوب
</div>

تُنفَّذ الاستعلامات التي تستخدم `FINAL` ببطءٍ طفيف مقارنةً بالاستعلامات المماثلة التي لا تستخدم `FINAL`، وذلك لأن:

* تُدمَج البيانات أثناء تنفيذ الاستعلام.
* قد تقرأ الاستعلامات التي تستخدم `FINAL` أعمدة المفتاح الأساسي بالإضافة إلى الأعمدة المحددة في الاستعلام.

يتطلب `FINAL` موارد حوسبة وذاكرة إضافية، لأن المعالجة التي تحدث عادةً وقت الدمج يجب أن تتم في الذاكرة عند تنفيذ الاستعلام. ومع ذلك، يكون استخدام `FINAL` ضروريًا أحيانًا للحصول على نتائج دقيقة (إذ قد لا تكون البيانات قد دُمجت بالكامل بعد). كما أنه أقل تكلفة من تشغيل `OPTIMIZE` لفرض الدمج.

وكبديل لاستخدام `FINAL`، يمكن أحيانًا استخدام استعلامات مختلفة تفترض أن العمليات الخلفية لمحرك `MergeTree` لم تحدث بعد، والتعامل مع ذلك بتطبيق التجميع (على سبيل المثال، لتجاهل التكرارات). وإذا كنت بحاجة إلى استخدام `FINAL` في استعلاماتك للحصول على النتائج المطلوبة، فلا بأس بذلك، لكن انتبه إلى المعالجة الإضافية التي يتطلبها.

يمكن تطبيق `FINAL` تلقائيًا باستخدام إعداد [FINAL](../../../operations/settings/settings.md#final) على جميع الجداول في الاستعلام، عبر جلسة أو ملف تعريف مستخدم.

<div id="example-usage">
  ### مثال للاستخدام
</div>

استخدام الكلمة المفتاحية `FINAL`

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

استخدام `FINAL` كإعداد خاص بالاستعلام

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

استخدام `FINAL` كإعداد على مستوى الجلسة

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### الأسماء المستعارة وFINAL
</div>

عندما يكون للجدول اسم مستعار، يأتي `FINAL` بعده. ويتضح ذلك بوضوح خاصةً في استعلامات [`JOIN`](/ar/sql-reference/statements/select/join)، إذ تُعطى الجداول عادةً أسماءً مستعارة:

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` هو مُعدِّل على مرجع الجدول، لذا يجب أن يأتي بعد التعبير الكامل `table [AS alias]`. ووضعه قبل الاسم المستعار (`FROM table1 FINAL AS t1`) يُعدّ خطأً في بناء الجملة.

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

إذا حُذفت عبارة `FROM`، فستُقرأ البيانات من جدول `system.one`.
يحتوي جدول `system.one` على صف واحد فقط (ويؤدي هذا الجدول الغرض نفسه الذي يؤديه جدول DUAL الموجود في أنظمة إدارة قواعد البيانات الأخرى).

لتنفيذ استعلام، تُستخرج جميع الأعمدة المدرجة فيه من الجدول المناسب. وتُستبعد من الاستعلامات الفرعية أي أعمدة لا يحتاج إليها الاستعلام الخارجي.
إذا لم يُدرج الاستعلام أي أعمدة (على سبيل المثال، `SELECT count() FROM t`)، فسيُستخرج أحد الأعمدة من الجدول على أي حال (ويُفضَّل أصغرها)، وذلك لحساب عدد الصفوف.