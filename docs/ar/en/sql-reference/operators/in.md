---
description: 'توثيق لعوامل التشغيل IN باستثناء عوامل التشغيل NOT IN وGLOBAL IN وGLOBAL
  NOT IN التي تُغطَّى بشكل منفصل'
slug: /sql-reference/operators/in
title: 'عوامل التشغيل IN'
doc_type: 'reference'
---

يتم تناول عوامل التشغيل `IN` و`NOT IN` و`GLOBAL IN` و`GLOBAL NOT IN` بشكل منفصل، نظرًا لغنى وظائفها وتعدد إمكاناتها.

الجانب الأيسر من المعامل هو إما عمود واحد أو Tuple.

أمثلة:

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

إذا كان الجانب الأيسر عموداً واحداً مُدرَجاً في الفهرس، والجانب الأيمن مجموعةً من الثوابت، استخدم النظام الفهرسَ لمعالجة الاستعلام.

لا تُدرج قيمًا كثيرة جدًا بصورة صريحة (أي الملايين). إذا كانت مجموعة البيانات كبيرة، فضعها في جدول مؤقت (على سبيل المثال، راجع القسم [البيانات الخارجية لمعالجة الاستعلامات](../../engines/table-engines/special/external-data.md))، ثم استخدم استعلامًا فرعيًا.

يمكن أن يكون الجانب الأيمن من المعامل مجموعةً من التعبيرات الثابتة، أو مجموعةً من Tuples تحتوي على تعبيرات ثابتة (كما هو موضح في الأمثلة أعلاه)، أو اسم جدول في قاعدة البيانات، أو استعلامًا فرعيًا من نوع `SELECT` بين قوسين.

للتوافق مع الإصدارات السابقة، عندما يكون الجانب الأيمن تعبير `tuple` مفرداً، يمكن تفسيره إما كمجموعة من القيم أو كقيمة tuple واحدة، وذلك بحسب الجانب الأيسر من عامل التشغيل `IN`. إذا كان الجانب الأيسر قيمةً عددية مفردة (scalar)، فإن ClickHouse يعامل عناصر تعبير `tuple` المفرد هذا على الجانب الأيمن كقيم `IN` منفصلة:

```sql title="Query"
SELECT
    1 IN (tuple(1, 2)) AS one_in_tuple,
    2 IN (tuple(1, 2)) AS two_in_tuple,
    3 IN (tuple(1, 2)) AS three_in_tuple;
```

```text title="Response"
┌─one_in_tuple─┬─two_in_tuple─┬─three_in_tuple─┐
│            1 │            1 │              0 │
└──────────────┴──────────────┴────────────────┘
```

يعمل هذا مثل `SELECT 1 IN (1, 2)`. إذا كان الجانب الأيسر أيضاً Tuple، فإن الجانب الأيمن يُفسَّر باعتباره مجموعة من قيم Tuple:

```sql title="Query"
SELECT tuple(1, 2) IN (tuple(1, 2)) AS tuple_in_tuple;
```

```text title="Response"
┌─tuple_in_tuple─┐
│              1 │
└────────────────┘
```

تنطبق هذه المعالجة الخاصة فقط عندما يكون الجانب الأيمن تعبير `tuple` واحدًا. لا يمكن مطابقة الجانب الأيسر القياسي (scalar) مع جانب أيمن يحتوي على قيم tuple متعددة:

```sql title="Query"
SELECT 1 IN (tuple(1, 2), tuple(3, 4));
```

```text title="Response"
Code: 43. DB::Exception: Unsupported types for IN. First argument type UInt8. Second argument type Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8)). (ILLEGAL_TYPE_OF_ARGUMENT)
```

يتيح ClickHouse اختلاف الأنواع بين الجانب الأيسر والجانب الأيمن في الاستعلام الفرعي `IN`.
في هذه الحالة، يُحوِّل النظام قيمة الجانب الأيمن إلى نوع الجانب الأيسر،
كما لو طُبِّقت الدالة [accurateCastOrNull](/ar/sql-reference/functions/type-conversion-functions#accurateCastOrNull) على الجانب الأيمن.

يعني هذا أن نوع البيانات يصبح [Nullable](../../sql-reference/data-types/nullable.md)، وإذا تعذّر إجراء التحويل، فإنه يُرجع [NULL](/ar/operations/settings/formats#input_format_null_as_default).

**مثال**

```sql title="Query"
SELECT '1' IN (SELECT 1);
```

```text title="Response"
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

إذا كان الجانب الأيمن من المعامل هو اسم جدول (على سبيل المثال، `UserID IN users`)، فهذا يعادل الاستعلام الفرعي `UserID IN (SELECT * FROM users)`. استخدم هذا عند التعامل مع البيانات الخارجية المُرسَلة مع الاستعلام. على سبيل المثال، يمكن إرسال الاستعلام مصحوبًا بمجموعة من معرّفات المستخدمين المحمَّلة في الجدول المؤقت &#39;users&#39;، والتي يجب تصفيتها.

إذا كان الجانب الأيمن من المعامل اسمَ جدول يستخدم محرك Set (مجموعة بيانات جاهزة تُخزَّن دائمًا في ذاكرة RAM)، فلن تُعاد إنشاء مجموعة البيانات لكل استعلام من جديد.

قد يحدد الاستعلام الفرعي أكثر من عمود واحد لتصفية المجموعات.

مثال:

```sql title="Query"
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

يجب أن تكون الأعمدة الواقعة على يسار عامل التشغيل `IN` ويمينه من النوع نفسه.

يمكن أن يَرِد عامل التشغيل `IN` والاستعلام الفرعي في أي جزء من الاستعلام، بما في ذلك داخل الدوال التجميعية ودوال لامبدا.
مثال:

```sql title="Query"
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

```text title="Response"
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

لكل يوم بعد 17 مارس، احسب النسبة المئوية لمشاهدات الصفحات التي نفّذها المستخدمون الذين زاروا الموقع في 17 مارس.
يُنفَّذ الاستعلام الفرعي في عبارة `IN` دائمًا مرة واحدة فقط وعلى خادم واحد. ولا توجد استعلامات فرعية معتمدة.

<div id="null-processing">
  ## معالجة NULL
</div>

أثناء معالجة الطلب، يفترض عامل التشغيل `IN` أن نتيجة أي عملية تتضمن [NULL](/ar/operations/settings/formats#input_format_null_as_default) تساوي دائمًا `0`، سواء جاءت `NULL` على يمين عامل التشغيل أو على يساره. لا تُضمَّن قيم `NULL` في أي مجموعة بيانات، ولا تقابل بعضها بعضًا، ولا يمكن مقارنتها إذا كان [transform&#95;null&#95;in = 0](../../operations/settings/settings.md#transform_null_in).

فيما يلي مثال باستخدام الجدول `t_null`:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

يؤدي تنفيذ الاستعلام `SELECT x FROM t_null WHERE y IN (NULL,3)` إلى النتيجة التالية:

```text
┌─x─┐
│ 2 │
└───┘
```

يمكنك أن ترى أن الصف الذي تكون فيه `y = NULL` يُستبعَد من نتائج الاستعلام. ويعود ذلك إلى أن ClickHouse لا يستطيع تحديد ما إذا كانت `NULL` مُضمَّنة في المجموعة `(NULL,3)`، لذلك يُرجِع `0` كنتيجة لهذه العملية، ثم يستبعد `SELECT` هذا الصف من المخرجات النهائية.

```sql
SELECT y IN (NULL, 3)
FROM t_null
```

```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

<div id="distributed-subqueries">
  ## الاستعلامات الفرعية الموزعة
</div>

ثمة خياران لعامل التشغيل `IN` مع الاستعلامات الفرعية (على غرار عوامل تشغيل `JOIN`): `IN` / `JOIN` العادي و`GLOBAL IN` / `GLOBAL JOIN`. ويكمن الفرق بينهما في طريقة تنفيذهما عند معالجة الاستعلامات الموزعة.

:::note
تذكّر أن الخوارزميات الموضحة أدناه قد تعمل بشكل مختلف وفقًا لإعداد [الإعدادات](../../operations/settings/settings.md) `distributed_product_mode`.
:::

عند استخدام `IN` العادية، يُرسَل الاستعلام إلى الخوادم البعيدة، وتُنفِّذ كلٌّ منها الاستعلامات الفرعية الواردة في عبارة `IN` أو `JOIN`.

عند استخدام `GLOBAL IN` / `GLOBAL JOIN`، تُنفَّذ أولاً جميع الاستعلامات الفرعية الخاصة بـ `GLOBAL IN` / `GLOBAL JOIN`، وتُجمَّع نتائجها في جداول مؤقتة. ثم تُرسَل هذه الجداول المؤقتة إلى كل خادم بعيد، حيث تُنفَّذ الاستعلامات باستخدام هذه البيانات المؤقتة.

بالنسبة لـ `GLOBAL ... JOIN`، يعتمد الجانب الذي يُحسب كاستعلام فرعي على نوع الـ join: ففي حالتَي `LEFT` و`INNER`، يُحسب الجدول الأيمن؛ أما في حالة `RIGHT`، فيُحسب الجدول الأيسر بدلاً من ذلك، إذ إن الجدول الأيمن هو الجانب المحفوظ وينبغي قراءته من الأجزاء (shards).

بالنسبة للاستعلام غير الموزَّع، استخدم `IN` / `JOIN` العادي.

كن حذرًا عند استخدام الاستعلامات الفرعية في عبارتَي `IN` / `JOIN` عند معالجة الاستعلامات الموزعة.

لنستعرض بعض الأمثلة. افترض أن كل خادم في الكلستر يحتوي على جدول **local&#95;table** عادي. كما يحتوي كل خادم على جدول **distributed&#95;table** من نوع **Distributed**، الذي يشمل جميع الخوادم في الكلستر.

عند توجيه استعلام إلى **distributed&#95;table**، يُرسَل الاستعلام إلى جميع الخوادم البعيدة وينفَّذ عليها باستخدام **local&#95;table**.

على سبيل المثال، الاستعلام

```sql
SELECT uniq(UserID) FROM distributed_table
```

سيتم إرساله إلى جميع الخوادم البعيدة بوصفه

```sql
SELECT uniq(UserID) FROM local_table
```

وتُنفَّذ على كلٍّ منها بالتوازي، حتى تصل إلى المرحلة التي يمكن فيها دمج النتائج الوسيطة. عندئذٍ، تُعاد النتائج الوسيطة إلى الخادم الطالب ويجري دمجها عليه، ثم تُرسَل النتيجة النهائية إلى العميل.

لنفحص الآن استعلامًا يستخدم `IN`:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

* حساب تقاطع جمهور موقعين.

سيُرسَل هذا الاستعلام إلى جميع الخوادم البعيدة بوصفه

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

بمعنى آخر، سيتم تجميع مجموعة البيانات الموجودة في جملة `IN` على كل خادم بصورة مستقلة، من البيانات المخزنة محليًا على ذلك الخادم فحسب.

سيعمل هذا بشكل صحيح وبأفضل أداء إذا كنت مستعدًا لهذه الحالة وقد وزّعت البيانات على خوادم الكلستر بحيث تقع بيانات كل UserID بالكامل على خادم واحد. في هذه الحالة، ستتوفر جميع البيانات اللازمة محليًا على كل خادم. وإلا، فستكون النتيجة غير دقيقة. نشير إلى هذا الشكل من الاستعلام بـ &quot;local IN&quot;.

لتصحيح طريقة عمل الاستعلام عندما تكون البيانات موزَّعة عشوائيًا عبر خوادم الكلستر، يمكنك تحديد **distributed&#95;table** داخل استعلام فرعي. سيبدو الاستعلام كالتالي:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

سيُرسَل هذا الاستعلام إلى جميع الخوادم البعيدة بوصفه

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

سيبدأ الاستعلام الفرعي بالتنفيذ على كل خادم بعيد. ونظرًا لأن الاستعلام الفرعي يستخدم جدولًا موزعًا، فسيُعاد إرسال الاستعلام الفرعي الموجود على كل خادم بعيد إلى جميع الخوادم البعيدة على النحو الآتي:

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

على سبيل المثال، إذا كان لديك مجموعة مكوّنة من 100 خادم، فإن تنفيذ الاستعلام بالكامل سيستلزم 10,000 طلب أولي، وهو أمر غير مقبول بشكل عام.

في مثل هذه الحالات، يجب دائمًا استخدام `GLOBAL IN` بدلًا من `IN`. لنرَ كيف يعمل مع الاستعلام:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

سيُنفّذ الخادم الطالب الاستعلام الفرعي:

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

وسيتم وضع النتيجة في جدول مؤقت في ذاكرة الوصول العشوائي (RAM). ثم سيُرسَل الطلب إلى كل خادم بعيد على النحو التالي:

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

سيُرسَل الجدول المؤقت `_data1` إلى كل خادم بعيد مع الاستعلام (اسم الجدول المؤقت يعتمد على التنفيذ).

وهذا أكثر كفاءة من استخدام `IN` العادي. ومع ذلك، ضع النقاط التالية في الاعتبار:

1. عند إنشاء جدول مؤقت، لا تُزال القيم المكررة من البيانات. لتقليل حجم البيانات المنقولة عبر الشبكة، حدِّد DISTINCT في الاستعلام الفرعي. (ولا تحتاج إلى ذلك مع `IN` العادي.)
2. سيُرسَل الجدول المؤقت إلى جميع الخوادم البعيدة. ولا يراعي النقل طوبولوجيا الشبكة. على سبيل المثال، إذا كانت هناك 10 خوادم بعيدة موجودة في مركز بيانات بعيد جدًا عن الخادم الطالب، فستُرسَل البيانات 10 مرات عبر القناة إلى مركز البيانات البعيد. حاول تجنب مجموعات البيانات الكبيرة عند استخدام `GLOBAL IN`.
3. عند نقل البيانات إلى الخوادم البعيدة، لا يمكن ضبط القيود المفروضة على عرض النطاق الترددي للشبكة. وقد يؤدي ذلك إلى إرهاق الشبكة.
4. حاول توزيع البيانات على الخوادم بحيث لا تضطر إلى استخدام `GLOBAL IN` بانتظام.
5. إذا كنت بحاجة إلى استخدام `GLOBAL IN` كثيرًا، فخطط لموضع عنقود ClickHouse بحيث توجد مجموعة واحدة من النسخ المتماثلة في مركز بيانات واحد فقط، مع شبكة سريعة بينها، بحيث يمكن معالجة الاستعلام بالكامل داخل مركز بيانات واحد.

ومن المنطقي أيضًا تحديد جدول محلي في عبارة `GLOBAL IN` إذا كان هذا الجدول المحلي متاحًا فقط على الخادم الطالب وكنت تريد استخدام بياناته على الخوادم البعيدة.

<div id="distributed-subqueries-and-max_rows_in_set">
  ### الاستعلامات الفرعية الموزعة و max_rows_in_set
</div>

يمكنك استخدام [`max_rows_in_set`](/ar/operations/settings/settings#max_rows_in_set) و [`max_bytes_in_set`](/ar/operations/settings/settings#max_bytes_in_set) للتحكم في مقدار البيانات المنقولة أثناء الاستعلامات الموزعة.

ويكتسب هذا أهمية خاصة إذا كان استعلام `GLOBAL IN` يعيد كمية كبيرة من البيانات. تأمل عبارة SQL التالية:

```sql
SELECT * FROM table1 WHERE col1 GLOBAL IN (SELECT col1 FROM table2 WHERE <some_predicate>)
```

إذا لم يكن `some_predicate` انتقائيًا بما يكفي، فسيُرجع كمية كبيرة من البيانات، مما يسبب مشكلات في الأداء. في مثل هذه الحالات، من الأفضل الحد من نقل البيانات عبر الشبكة. لاحظ أيضًا أن [`set_overflow_mode`](/ar/operations/settings/settings#set_overflow_mode) مضبوط على `throw` (افتراضيًا)، ما يعني أنه يتم إطلاق استثناء عند بلوغ هذه العتبات.

<div id="distributed-subqueries-and-max_parallel_replicas">
  ### الاستعلامات الفرعية الموزعة و max_parallel_replicas
</div>

عندما تكون [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) أكبر من 1، يُجرى تحويل إضافي على الاستعلامات الموزعة.

على سبيل المثال، ما يلي:

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

تُحوَّل في كل خادم إلى:

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

حيث تكون `M` بين `1` و`3` بحسب أي نسخة متماثلة يُنفَّذ عليها الاستعلام المحلي.

تؤثر هذه الإعدادات في كل جدول من عائلة MergeTree ضمن query، ولها التأثير نفسه كما لو تم تطبيق `SAMPLE 1/3 OFFSET (M-1)/3` على كل جدول.

لذلك، فإن إضافة الإعداد [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) لن تعطي نتائج صحيحة إلا إذا كان لكلتا الجدولين مخطط replication نفسه، وكانت sampling تتم فيهما بواسطة UserID أو مفتاح فرعي له. وعلى وجه الخصوص، إذا لم يكن لدى `local_table_2` مفتاح sampling، فستنتج نتائج غير صحيحة. وتنطبق القاعدة نفسها على `JOIN`.

أحد الحلول البديلة، إذا كانت `local_table_2` لا تستوفي المتطلبات، هو استخدام `GLOBAL IN` أو `GLOBAL JOIN`.

إذا لم يكن لدى جدول مفتاح sampling، فيمكن استخدام خيارات أكثر مرونة لـ [parallel&#95;replicas&#95;custom&#95;key](/ar/operations/settings/settings#parallel_replicas_custom_key)، والتي قد تنتج سلوكًا مختلفًا وأكثر كفاءة.