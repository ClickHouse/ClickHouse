---
description: 'صفحة تفصّل محلّل الاستعلامات في ClickHouse'
keywords: ['محلّل الاستعلامات']
sidebar_label: 'محلّل الاستعلامات'
slug: /operations/analyzer
title: 'محلّل الاستعلامات'
doc_type: 'مرجع'
---

في الإصدار `24.3` من ClickHouse، جرى تفعيل محلّل الاستعلامات الجديد افتراضيًا.
يمكنك الاطّلاع على مزيد من التفاصيل حول كيفية عمله [هنا](/ar/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

<div id="known-incompatibilities">
  ## حالات عدم التوافق المعروفة
</div>

على الرغم من إصلاح عدد كبير من الأخطاء وإدخال تحسينات جديدة، فإنها تجلب أيضًا بعض التغييرات غير المتوافقة في سلوك ClickHouse. يُرجى قراءة التغييرات التالية لمعرفة كيفية إعادة كتابة استعلاماتك لتتوافق مع المحلِّل.

<div id="invalid-queries-are-no-longer-optimized">
  ### لم تعد الاستعلامات غير الصالحة تُحسَّن
</div>

كانت البنية السابقة لتخطيط الاستعلامات تُجري تحسينات على مستوى AST قبل خطوة التحقق من صحة الاستعلام.
وكانت هذه التحسينات قادرة على إعادة كتابة الاستعلام الأصلي ليصبح صالحًا وقابلًا للتنفيذ.

في المُحلِّل، يجري التحقق من صحة الاستعلام قبل خطوة التحسين.
وهذا يعني أن الاستعلامات غير الصالحة التي كان يمكن تنفيذها سابقًا لم تعد مدعومة.
وفي مثل هذه الحالات، يجب إصلاح الاستعلام يدويًا.

<div id="example-1">
  #### مثال 1
</div>

يستخدم الاستعلام التالي العمود `number` في قائمة الإسقاط، مع أن المتاح بعد التجميع هو `toString(number)` فقط.
في المحلل القديم، كان `GROUP BY toString(number)` يُحسَّن إلى `GROUP BY number,`، مما يجعل الاستعلام صحيحًا.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### المثال 2
</div>

تحدث المشكلة نفسها في هذا الاستعلام. يُستخدم العمود `number` بعد التجميع مع مفتاح آخر.
كان محلل الاستعلامات السابق يصحّح هذا الاستعلام عبر نقل شرط التصفية `number > 5` من عبارة `HAVING` إلى عبارة `WHERE`.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

لإصلاح الاستعلام، ينبغي نقل جميع الشروط التي تنطبق على الأعمدة غير المجمّعة إلى قسم `WHERE` بما يتوافق مع صياغة SQL القياسية:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### `CREATE VIEW` مع استعلام غير صالح
</div>

يُجري المحلّل دائمًا تحقّقًا من الأنواع.
في السابق، كان من الممكن إنشاء `VIEW` باستخدام استعلام `SELECT` غير صالح.
وكان ذلك يفشل عند تنفيذ أول `SELECT` أو `INSERT` (في حالة `MATERIALIZED VIEW`).

لم يعد من الممكن إنشاء `VIEW` بهذه الطريقة.

<div id="example-view">
  #### مثال
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### أوجه عدم التوافق المعروفة في عبارة `JOIN`
</div>

<div id="join-using-column-from-projection">
  #### `JOIN` باستخدام عمود من الإسقاط
</div>

لا يمكن استخدام اسم مستعار من قائمة `SELECT` كمفتاح `JOIN USING` بشكل افتراضي.

عند تفعيل الإعداد الجديد `analyzer_compatibility_join_using_top_level_identifier`، يتغيّر سلوك `JOIN USING` بحيث يُفضِّل تفسير المعرّفات استنادًا إلى التعبيرات الواردة في قائمة الإسقاط ضمن استعلام `SELECT`، بدلًا من استخدام أعمدة الجدول الأيسر مباشرةً.

على سبيل المثال:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

عند ضبط `analyzer_compatibility_join_using_top_level_identifier` على `true`، يُفسَّر شرط الربط على أنه `t1.a + 1 = t2.b`، بما يتوافق مع سلوك الإصدارات السابقة.
ستكون النتيجة `2, 'two'`.
وعندما يكون الإعداد `false`، يكون شرط الربط افتراضيًا `t1.b = t2.b`، وسيُرجع الاستعلام `2, 'one'`.
إذا لم يكن `b` موجودًا في `t1`، فسيفشل الاستعلام مع ظهور خطأ.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### تغيّرات السلوك مع `JOIN USING` والأعمدة `ALIAS`/`MATERIALIZED`
</div>

في المُحلِّل، يؤدّي استخدام `*` في استعلام `JOIN USING` يتضمّن أعمدة `ALIAS` أو `MATERIALIZED` إلى إدراج هذه الأعمدة في مجموعة النتائج افتراضيًا.

على سبيل المثال:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

في المحلل، ستتضمن نتيجة هذا الاستعلام العمود `payload` إلى جانب `id` من الجدولين كليهما.
في المقابل، كان المحلل السابق لا يضمّن أعمدة `ALIAS` هذه إلا إذا كانت إعدادات محددة (`asterisk_include_alias_columns` أو `asterisk_include_materialized_columns`) مفعّلة،
وقد تظهر الأعمدة بترتيب مختلف.

ولضمان نتائج متسقة ومتوقعة، خاصةً عند ترحيل الاستعلامات القديمة إلى المحلل، يُنصح بتحديد الأعمدة صراحةً في عبارة `SELECT` بدلاً من استخدام `*`.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### التعامل مع معدِّلات الأنواع للأعمدة في عبارة `USING`
</div>

في الإصدار الجديد من المحلِّل، جرى توحيد القواعد الخاصة بتحديد النوع الأعلى المشترك للأعمدة المحددة في عبارة `USING` لإنتاج نتائج أكثر قابلية للتوقع،
وخاصة عند التعامل مع معدِّلات الأنواع مثل `LowCardinality` و`Nullable`.

* `LowCardinality(T)` و`T`: عند ربط عمود من النوع `LowCardinality(T)` بعمود من النوع `T`، سيكون النوع الأعلى المشترك الناتج هو `T`، ما يعني عمليًا إسقاط معدِّل `LowCardinality`.
* `Nullable(T)` و`T`: عند ربط عمود من النوع `Nullable(T)` بعمود من النوع `T`، سيكون النوع الأعلى المشترك الناتج هو `Nullable(T)`، مما يضمن الحفاظ على قابلية قبول القيم الفارغة.

على سبيل المثال:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

في هذا الاستعلام، يُحدَّد النوع الأعلى المشترك لـ `id` على أنه `String`، مع تجاهل المُعدِّل `LowCardinality` في `t1`.

<div id="projection-column-names-changes">
  ### تغييرات أسماء أعمدة الإسقاط
</div>

عند احتساب أسماء الإسقاطات، لا يجري استبدال الأسماء المستعارة.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### أنواع وسائط الدالة غير المتوافقة
</div>

في المحلّل، يحدث استنتاج النوع أثناء تحليل الاستعلام الأولي.
ويعني هذا التغيير أن التحقّق من الأنواع يُجرى قبل التقييم المختصر؛ لذلك، يجب أن يكون لوسائط الدالة `if` دائمًا نوع أعلى مشترك.

على سبيل المثال، يفشل الاستعلام التالي مع ظهور `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### العناقيد غير المتجانسة
</div>

يُغيّر المحلِّل بروتوكول الاتصال بين الخوادم داخل العنقود تغييرًا كبيرًا. لذلك، يستحيل تشغيل الاستعلامات الموزعة على خوادم تختلف فيها قيم الإعداد `enable_analyzer`.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### تُعالَج عمليات التعديل بواسطة المحلّل السابق
</div>

لا تزال عمليات التعديل تستخدم المحلّل القديم.
وهذا يعني أنه لا يمكن استخدام بعض ميزات ClickHouse SQL الجديدة في عمليات التعديل. على سبيل المثال، عبارة `QUALIFY`.
يمكن الاطّلاع على الحالة [هنا](https://github.com/ClickHouse/ClickHouse/issues/61563).

<div id="unsupported-features">
  ### الميزات غير المدعومة
</div>

فيما يلي قائمة بالميزات التي لا يدعمها المحلّل حاليًا:

* فهرس Annoy.
* فهرس Hypothesis. العمل عليه جارٍ [هنا](https://github.com/ClickHouse/ClickHouse/pull/48381).
* ‏window view غير مدعومة. ولا توجد خطط لدعمها مستقبلًا.

<div id="cloud-migration">
  ## الترحيل إلى Cloud
</div>

نعمل على تفعيل محلّل الاستعلامات الجديد على جميع المثيلات التي يكون معطّلًا فيها حاليًا، دعمًا لتحسينات جديدة في الوظائف والأداء. يفرض هذا التغيير قواعد أكثر صرامة لنطاق SQL، ما يتطلب من العملاء تحديث الاستعلامات غير المتوافقة يدويًا.

<div id="migration-workflow">
  ### سير عمل الترحيل
</div>

1. حدِّد الاستعلام من خلال تصفية `system.query_log` باستخدام `normalized_query_hash`:

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. نفِّذ الاستعلام بعد تمكين المُحلِّل بإضافة هذه الإعدادات.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. أعد صياغة الاستعلام وتحقّق من نتائج الاستعلام للتأكد من أنها تطابق المخرجات الناتجة عند تعطيل المحلّل.

يُرجى الرجوع إلى أكثر حالات عدم التوافق شيوعًا التي تمت مواجهتها أثناء الاختبار الداخلي.

<div id="unknown-expression-identifier">
  ### معرّف التعبير غير المعروف
</div>

الخطأ: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. رمز الاستثناء: 47

السبب: إن الاستعلامات التي تعتمد على سلوكيات قديمة متساهلة وغير قياسية، مثل الإشارة إلى الأسماء المستعارة المحسوبة داخل شروط التصفية، أو الإسقاطات الملتبسة في الاستعلامات الفرعية، أو النطاق &quot;dynamic&quot; لتعبير الجدول الشائع، تُعرَّف الآن على نحو صحيح بأنها غير صالحة ويُرفض تنفيذها فورًا.

الحل: حدّث أنماط SQL لديك كما يلي:

* منطق التصفية: انقل المنطق من WHERE إلى HAVING عند التصفية بناءً على النتائج، أو كرّر التعبير في WHERE عند التصفية على بيانات المصدر.
* نطاق الاستعلام الفرعي: حدّد صراحةً جميع الأعمدة التي يحتاجها الاستعلام الخارجي.
* مفاتيح JOIN: استخدم ON مع التعبيرات الكاملة بدلًا من USING إذا كان المفتاح اسمًا مستعارًا.
* في الاستعلامات الخارجية، ارجع إلى الاسم المستعار الخاص بالاستعلام الفرعي/تعبير الجدول الشائع نفسه، لا إلى الجداول الموجودة بداخله.

<div id="non-aggregated-columns-in-group-by">
  ### الأعمدة غير المجمّعة في GROUP BY
</div>

الخطأ: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. رمز الاستثناء: 215

السبب: كان المحلِّل القديم يسمح باختيار أعمدة غير موجودة في بند GROUP BY (وغالبًا ما كان يلتقط قيمة عشوائية). يلتزم المحلِّل بمعيار SQL القياسي: يجب أن يكون كل عمود مُختار إما ضمن دالة تجميعية أو مفتاح تجميع.

الحل: ضَع العمود داخل `any()` أو `argMax()`، أو أضِفه إلى GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### أسماء تعبيرات الجدول الشائعة المتكررة
</div>

الخطأ: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. رمز الاستثناء: 179

السبب: كان المُحلِّل القديم يسمح بتعريف عدة تعبيرات الجدول الشائعة (WITH ...) بالاسم نفسه، بحيث يَحجب التعريف اللاحق التعريف السابق. ويمنع المُحلِّل هذا الالتباس.

الحل: أعد تسمية تعبيرات الجدول الشائعة المكررة بحيث يحمل كلٌّ منها اسمًا فريدًا.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### معرّفات الأعمدة المبهمة
</div>

الخطأ: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` رمز الاستثناء: 207

السبب: يشير الاستعلام إلى اسم عمود موجود في عدة جداول ضمن JOIN من دون تحديد الجدول المصدر. كان المحلّل القديم غالبًا ما يستنتج العمود استنادًا إلى منطق داخلي، لكن المحلّل يتطلب اسمًا صريحًا.

الحل: حدِّد اسم العمود بالكامل باستخدام table&#95;alias.column&#95;name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### استخدام غير صحيح لـ FINAL
</div>

الخطأ: `Table expression modifiers FINAL are not supported for subquery...` أو `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). رموز الاستثناء: 1، 181

السبب: FINAL هو مُعدِّل لتخزين الجدول (وتحديدًا [Shared]ReplacingMergeTree). يرفض المحلّل FINAL عند تطبيقه على:

* الاستعلامات الفرعية أو الجداول المشتقة (مثل FROM (SELECT ...) FINAL).
* محركات الجداول التي لا تدعمه (مثل SharedMergeTree).

الحل: طبّق FINAL فقط على الجدول المصدر داخل الاستعلام الفرعي، أو أزِله إذا كان المحرك لا يدعمه.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### عدم التمييز بين حالة الأحرف في الدالة `countDistinct()`
</div>

الخطأ: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. رمز الاستثناء: 46

السبب: أسماء الدوال حساسة لحالة الأحرف أو تكون مطابقةً بشكل صارم في المحلّل. لم يعد `countdistinct` (بأحرف صغيرة بالكامل) يُفسَّر تلقائيًا.

الحل: استخدم الصيغة القياسية `countDistinct` (camelCase) أو الدالة `uniq` الخاصة بـ ClickHouse.