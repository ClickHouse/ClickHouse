---
description: 'دليل لاستخدام ميزة ذاكرة التخزين المؤقت للاستعلامات في ClickHouse وتهيئتها'
sidebar_label: 'ذاكرة التخزين المؤقت للاستعلامات'
sidebar_position: 65
slug: /operations/query-cache
title: 'ذاكرة التخزين المؤقت للاستعلامات'
doc_type: 'guide'
---

تتيح ذاكرة التخزين المؤقت للاستعلامات احتساب استعلامات `SELECT` مرة واحدة فقط، ثم تقديم عمليات التنفيذ اللاحقة للاستعلام نفسه مباشرةً من الذاكرة المؤقتة.
وبحسب نوع الاستعلامات، يمكن أن يؤدي ذلك إلى خفض زمن الوصول واستهلاك الموارد على خادم ClickHouse بشكل كبير.

<div id="background-design-and-limitations">
  ## الخلفية والتصميم والقيود
</div>

يمكن عمومًا اعتبار ذاكرات التخزين المؤقت للاستعلامات إما متسقة معاملاتيًا أو غير متسقة معاملاتيًا.

* في ذاكرات التخزين المؤقت المتسقة معاملاتيًا، تُبطِل قاعدة البيانات نتائج الاستعلام المخزنة مؤقتًا (أي تتخلص منها) إذا تغيّرت نتيجة استعلام `SELECT`
  أو كان من المحتمل أن تتغيّر. في ClickHouse، تشمل العمليات التي تغيّر البيانات عمليات الإدراج/التحديث/الحذف في الجداول أو عليها أو منها، أو عمليات الدمج
  من نوع collapsing. ويُعدّ التخزين المؤقت المتسق معاملاتيًا مناسبًا بشكل خاص لقواعد بيانات OLTP، مثل
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (التي أزالت ذاكرة التخزين المؤقت للاستعلام بعد v8.0) و
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
* في ذاكرات التخزين المؤقت غير المتسقة معاملاتيًا، يُتسامح مع قدر طفيف من عدم الدقة في نتائج الاستعلامات على أساس أن جميع عناصر ذاكرة التخزين المؤقت
  تُمنَح مدة صلاحية تنتهي بعدها (مثلًا دقيقة واحدة)، وأن البيانات الأساسية لا تتغير إلا بقدر محدود خلال هذه المدة.
  ويُعد هذا النهج أنسب عمومًا لقواعد بيانات OLAP. وكمثال على حالة يكون فيها التخزين المؤقت غير المتسق معاملاتيًا كافيًا،
  تأمّل تقرير مبيعات يُحدَّث كل ساعة في أداة إعداد تقارير ويصل إليه عدة مستخدمين في الوقت نفسه. فعادةً ما تتغير بيانات المبيعات
  ببطء كافٍ بحيث لا تحتاج قاعدة البيانات إلا إلى احتساب التقرير مرة واحدة (ويمثل ذلك أول استعلام `SELECT`). ويمكن بعد ذلك خدمة الاستعلامات اللاحقة
  مباشرةً من ذاكرة التخزين المؤقت للاستعلام. وفي هذا المثال، قد تكون مدة صلاحية معقولة هي 30 دقيقة.

يُوفَّر التخزين المؤقت غير المتسق معاملاتيًا تقليديًا من خلال أدوات client أو حِزم proxy (مثل
[chproxy](https://www.chproxy.org/configuration/caching/)) التي تتفاعل مع قاعدة البيانات. ونتيجة لذلك، كثيرًا ما يتكرر منطق التخزين المؤقت نفسه
والإعداد نفسه. ومع ذاكرة التخزين المؤقت للاستعلام في ClickHouse، ينتقل منطق التخزين المؤقت إلى جهة server. وهذا يقلل جهد الصيانة
ويتجنب الازدواجية.

<div id="configuration-settings-and-usage">
  ## إعدادات التكوين والاستخدام
</div>

:::note
في ClickHouse Cloud، يجب استخدام [إعدادات على مستوى الاستعلام](/ar/operations/settings/query-level) لتعديل إعدادات ذاكرة التخزين المؤقت للاستعلام. أمّا تعديل [إعدادات على مستوى التكوين](/ar/operations/configuration-files) فغير مدعوم حاليًا.
:::

:::note
يشغّل [clickhouse-local](utilities/clickhouse-local.md) استعلامًا واحدًا في كل مرة. وبما أن التخزين المؤقت لنتائج الاستعلامات لا يكون منطقيًا في هذه الحالة، فإن ذاكرة التخزين المؤقت لنتائج الاستعلامات تكون معطّلة في clickhouse-local.
:::

يمكن استخدام الإعداد [use&#95;query&#95;cache](/ar/operations/settings/settings#use_query_cache) للتحكم في ما إذا كان ينبغي لاستعلام معيّن أو لجميع استعلامات
الجلسة الحالية استخدام ذاكرة التخزين المؤقت للاستعلام. على سبيل المثال، أول تنفيذ للاستعلام

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

سيُخزِّن نتيجة الاستعلام في ذاكرة التخزين المؤقت للاستعلامات. وستقرأ عمليات التنفيذ اللاحقة للاستعلام نفسه (أيضًا مع المعلَمة `use_query_cache = true`) النتيجة المحسوبة من ذاكرة التخزين المؤقت وتُرجعها فورًا.

:::note
إن تعيين `use_query_cache` وجميع الإعدادات الأخرى المرتبطة بذاكرة التخزين المؤقت للاستعلامات لا يسري مفعولها إلا على عبارات `SELECT` المستقلة. وعلى وجه الخصوص،
فإن نتائج عبارات `SELECT` الموجَّهة إلى العروض المُنشأة باستخدام `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` لا تُخزَّن مؤقتًا ما لم تُنفَّذ عبارة `SELECT`
مع `SETTINGS use_query_cache = true`.
:::

يمكن ضبط كيفية استخدام ذاكرة التخزين المؤقت بمزيد من التفصيل باستخدام الإعدادين [enable&#95;writes&#95;to&#95;query&#95;cache](/ar/operations/settings/settings#enable_writes_to_query_cache)
و[enable&#95;reads&#95;from&#95;query&#95;cache](/ar/operations/settings/settings#enable_reads_from_query_cache) (وكلاهما `true` افتراضيًا). يتحكم الإعداد الأول
في ما إذا كانت نتائج الاستعلام ستُخزَّن في ذاكرة التخزين المؤقت، بينما يحدد الإعداد الثاني ما إذا كان ينبغي لقاعدة البيانات محاولة استرداد نتائج الاستعلام
من ذاكرة التخزين المؤقت. على سبيل المثال، سيستخدم الاستعلام التالي ذاكرة التخزين المؤقت استخدامًا سلبيًا فقط، أي سيحاول القراءة منها دون تخزين
نتيجته فيها:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

لأقصى قدر من التحكم، يُوصى عمومًا باستخدام الإعدادات `use_query_cache` و`enable_writes_to_query_cache` و
`enable_reads_from_query_cache` مع استعلامات محددة فقط. ويمكن أيضًا تمكين التخزين المؤقت على مستوى المستخدم أو الملف الشخصي (مثلًا عبر `SET
use_query_cache = true`)، ولكن ينبغي الانتباه إلى أن جميع استعلامات `SELECT` قد تُرجع عندئذٍ نتائج مخزنة مؤقتًا.

يمكن مسح ذاكرة التخزين المؤقت للاستعلام باستخدام التعليمة `SYSTEM CLEAR QUERY CACHE`. ويُعرَض محتوى ذاكرة التخزين المؤقت للاستعلام في جدول النظام
[system.query&#95;cache](system-tables/query_cache.md). ويظهر عدد مرات نجاح ذاكرة التخزين المؤقت للاستعلام وإخفاقها منذ بدء تشغيل قاعدة البيانات كحدثين
&quot;QueryCacheHits&quot; و&quot;QueryCacheMisses&quot; في جدول النظام [system.events](system-tables/events.md). ولا يُحدَّث كلا العدادين إلا من أجل
استعلامات `SELECT` التي تعمل مع الإعداد `use_query_cache = true`، أما الاستعلامات الأخرى فلا تؤثر في &quot;QueryCacheMisses&quot;. ويُظهر الحقل `query_cache_usage`
في جدول النظام [system.query&#95;log](system-tables/query_log.md) لكل استعلام مُنفَّذ ما إذا كانت نتيجة الاستعلام قد كُتبت إلى
ذاكرة التخزين المؤقت للاستعلام أو قُرئت منها. وتُظهر المقاييس `QueryCacheEntries` و`QueryCacheBytes` في جدول النظام
[system.metrics](system-tables/metrics.md) عدد الإدخالات / البايتات التي يحتويها ذاكرة التخزين المؤقت للاستعلام حاليًا.

يوجد ذاكرة التخزين المؤقت للاستعلام مرة واحدة لكل عملية خادم ClickHouse. ومع ذلك، لا تتم مشاركة النتائج المخزنة مؤقتًا بين المستخدمين افتراضيًا. ويمكن
تغيير ذلك (انظر أدناه)، لكن لا يُنصح بذلك لأسباب أمنية.

يُشار إلى نتائج الاستعلامات في ذاكرة التخزين المؤقت للاستعلام بواسطة [شجرة البنية المجرّدة (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) الخاصة
باستعلامها. وهذا يعني أن التخزين المؤقت لا يتأثر بالأحرف الكبيرة/الصغيرة، فعلى سبيل المثال يُعامَل `SELECT 1` و`select 1` على أنهما الاستعلام نفسه. ولجعل
المطابقة أكثر طبيعية، تُزال من شجرة البنية المجرّدة جميع الإعدادات على مستوى الاستعلام المتعلقة بـ ذاكرة التخزين المؤقت للاستعلام و[تنسيق المخرجات](settings/settings-formats.md))
.

إذا أُوقِف الاستعلام بسبب استثناء أو إلغاء من المستخدم، فلن يُكتَب أي إدخال في ذاكرة التخزين المؤقت للاستعلام.

يمكن تهيئة حجم ذاكرة التخزين المؤقت للاستعلام بالبايتات، والحد الأقصى لعدد إدخالات ذاكرة التخزين المؤقت، والحد الأقصى لحجم إدخالات ذاكرة التخزين المؤقت الفردية (بالبايتات وبالسجلات)
باستخدام [خيارات مختلفة لتهيئة الخادم](/ar/operations/server-configuration-parameters/settings#query_cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

من الممكن أيضًا تقييد استخدام ذاكرة التخزين المؤقت للاستعلامات لكل مستخدم على حدة باستخدام [ملفات تعريف الإعدادات](settings/settings-profiles.md) و[قيود
الإعدادات](settings/constraints-on-settings.md). وبشكل أكثر تحديدًا، يمكنك فرض حد أقصى لمقدار الذاكرة (بالبايت) الذي يُسمح للمستخدم
بتخصيصه في ذاكرة التخزين المؤقت للاستعلام، وكذلك الحد الأقصى لعدد نتائج الاستعلام المخزَّنة. وللقيام بذلك، حدِّد أولًا الإعدادين
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/ar/operations/settings/settings#query_cache_max_size_in_bytes) و
[query&#95;cache&#95;max&#95;entries](/ar/operations/settings/settings#query_cache_max_entries) في ملف تعريف مستخدم داخل `users.xml`، ثم اجعل كلا الإعدادين
للقراءة فقط:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

لتحديد الحد الأدنى للمدة التي يجب أن يستغرقها الاستعلام حتى يمكن تخزين نتيجته مؤقتًا، يمكنك استخدام الإعداد
[query&#95;cache&#95;min&#95;query&#95;duration](/ar/operations/settings/settings#query_cache_min_query_duration). على سبيل المثال، يمكن تخزين نتيجة الاستعلام

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

لا تُخزَّن النتيجة مؤقتًا إلا إذا استغرق الاستعلام أكثر من 5 ثوانٍ. ومن الممكن أيضًا تحديد عدد المرات التي يجب تشغيل الاستعلام فيها حتى تصبح نتيجته
مخزنة مؤقتًا — ولهذا استخدم الإعداد [query&#95;cache&#95;min&#95;query&#95;runs](/ar/operations/settings/settings#query_cache_min_query_runs).

تصبح العناصر في ذاكرة التخزين المؤقت للاستعلامات قديمة بعد فترة زمنية معينة (time-to-live). افتراضيًا، تكون هذه الفترة 60 ثانية، ولكن يمكن تحديد
قيمة مختلفة على مستوى الجلسة أو ملف التعريف أو الاستعلام باستخدام الإعداد [query&#95;cache&#95;ttl](/ar/operations/settings/settings#query_cache_ttl). وتُخرج ذاكرة التخزين
المؤقت للاستعلامات العناصر بشكل &quot;كسول&quot;، أي عندما يصبح عنصر ما قديمًا، لا تتم إزالته فورًا من ذاكرة التخزين المؤقت. وبدلًا من ذلك، عندما يُراد
إدراج عنصر جديد في ذاكرة التخزين المؤقت للاستعلامات، تتحقق قاعدة البيانات مما إذا كانت ذاكرة التخزين المؤقت تحتوي على مساحة خالية كافية للعنصر الجديد. وإذا لم يكن الأمر
كذلك، تحاول قاعدة البيانات إزالة جميع العناصر القديمة. وإذا ظلت ذاكرة التخزين المؤقت لا تحتوي على مساحة خالية كافية، فلا يتم إدراج العنصر الجديد.

إذا تم تشغيل الاستعلام عبر HTTP، فإن ClickHouse يضبط الترويسات `Age` و`Expires` لتتضمن عمر العنصر المخزن مؤقتًا (بالثواني) والطابع الزمني لانتهاء صلاحيته.

تكون العناصر في ذاكرة التخزين المؤقت للاستعلامات مضغوطة افتراضيًا. وهذا يقلل من إجمالي استهلاك الذاكرة على حساب بطء عمليات الكتابة إلى / القراءة
من ذاكرة التخزين المؤقت للاستعلامات. لتعطيل الضغط، استخدم الإعداد [query&#95;cache&#95;compress&#95;entries](/ar/operations/settings/settings#query_cache_compress_entries).

أحيانًا يكون من المفيد الاحتفاظ بعدة نتائج مخزنة مؤقتًا للاستعلام نفسه. ويمكن تحقيق ذلك باستخدام الإعداد
[query&#95;cache&#95;tag](/ar/operations/settings/settings#query_cache_tag) الذي يعمل كتسمية (أو مساحة اسم) لعناصر ذاكرة التخزين المؤقت للاستعلامات. وتتعامل ذاكرة التخزين المؤقت للاستعلامات
مع نتائج الاستعلام نفسه ذات الوسوم المختلفة على أنها نتائج مختلفة.

مثال على إنشاء ثلاثة عناصر مختلفة في ذاكرة التخزين المؤقت للاستعلامات للاستعلام نفسه:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

لإزالة الإدخالات ذات الوسم `tag` فقط من ذاكرة التخزين المؤقت للاستعلامات، يمكنك استخدام التعليمة `SYSTEM CLEAR QUERY CACHE TAG 'tag'`.

<div id="subquery-caching">
  ## التخزين المؤقت للاستعلامات الفرعية
</div>

افتراضيًا، لا ينتقل `use_query_cache` من الاستعلام الخارجي إلى الاستعلامات الفرعية. وهذا يعني أن على كل استعلام فرعي تفعيل التخزين المؤقت صراحةً:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

في هذا المثال، لا تُخزَّن مؤقتًا إلا نتيجة الاستعلام الفرعي الداخلي، أما الاستعلام الخارجي فلا يُخزَّن مؤقتًا.

لتمكين التخزين المؤقت لجميع الاستعلامات الفرعية دفعة واحدة، استخدم الإعداد `query_cache_for_subqueries`:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

لتعطيل التخزين المؤقت صراحةً لاستعلام فرعي معيّن مع تفعيل النشر المجمّع، اضبط `use_query_cache = false` لهذا الاستعلام الفرعي:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

تظهر cache entries الخاصة بالاستعلامات الفرعية في [system.query&#95;cache](system-tables/query_cache.md) مع `is_subquery = 1`. وينطبق الإعداد `query_cache_ttl` أيضًا على cache entries الخاصة بالاستعلامات الفرعية، ويمكن ضبطه لكل استعلام فرعي على حدة.

يقرأ ClickHouse بيانات الجدول في blocks من [max&#95;block&#95;size](/ar/operations/settings/settings#max_block_size) rows. وبسبب التصفية والتجميع
وما إلى ذلك، تكون blocks النتائج عادةً أصغر بكثير من &#39;max&#95;block&#95;size&#39;، ولكن توجد أيضًا حالات تكون فيها أكبر بكثير. يتحكم الإعداد
[query&#95;cache&#95;squash&#95;partial&#95;results](/ar/operations/settings/settings#query_cache_squash_partial_results) (مُمكَّن افتراضيًا) في ما إذا كانت blocks النتائج
تُدمَج (إذا كانت صغيرة جدًا) أو تُقسَّم (إذا كانت كبيرة) إلى blocks بحجم &#39;max&#95;block&#95;size&#39; قبل إدراجها في ذاكرة التخزين المؤقت للاستعلام لنتائج الاستعلامات.
يؤدي ذلك إلى تقليل أداء writes إلى ذاكرة التخزين المؤقت للاستعلام، لكنه يحسّن معدل Compression لـ cache entries ويوفر
granularity أكثر طبيعية للـ block عند تقديم query results لاحقًا من ذاكرة التخزين المؤقت للاستعلام.

ونتيجة لذلك، تخزّن ذاكرة التخزين المؤقت للاستعلام لكل query عدة blocks
نتائج جزئية. ومع أن هذا السلوك يُعد خيارًا افتراضيًا جيدًا، فإنه يمكن إيقافه باستخدام الإعداد
[query&#95;cache&#95;squash&#95;partial&#95;results](/ar/operations/settings/settings#query_cache_squash_partial_results).

كذلك، لا تُخزَّن نتائج queries التي تتضمن دوال non-deterministic مؤقتًا افتراضيًا. وتشمل هذه الدوال

* دوال Accessing إلى Dictionaries: [`dictGet()`](/ar/sql-reference/functions/ext-dict-functions) وغيرها.
* [user-defined functions](../sql-reference/statements/create/function.md) التي لا تحتوي على الوسم `<deterministic>true</deterministic>` في تعريف XML الخاص بها،
* الدوال التي تُرجع التاريخ أو الوقت الحالي: [`now()`](../sql-reference/functions/date-time-functions.md#now)،
  [`today()`](../sql-reference/functions/date-time-functions.md#today)،
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) وغيرها،
* الدوال التي تُرجع قيمًا عشوائية: [`randomString()`](../sql-reference/functions/random-functions.md#randomString)،
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) وغيرها،
* الدوال التي تعتمد نتيجتها على حجم وترتيب الـ chunks الداخلية المستخدمة في query processing:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) وغيرها،
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock)،
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference)،
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) وغيرها،
* الدوال التي تعتمد على البيئة: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser)،
  [`queryID()`](/ar/sql-reference/functions/other-functions#queryID)،
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) وغيرها.

لفرض تخزين نتائج queries التي تتضمن دوال non-deterministic مؤقتًا رغم ذلك، استخدم الإعداد
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/ar/operations/settings/settings#query_cache_nondeterministic_function_handling).

لا تُخزَّن نتائج queries التي تتضمن system tables (مثل [system.processes](system-tables/processes.md)&#96; أو
[information&#95;schema.tables](system-tables/information_schema.md)) مؤقتًا افتراضيًا. ولفرض تخزين نتائج queries التي تتضمن
system tables مؤقتًا رغم ذلك، استخدم الإعداد [query&#95;cache&#95;system&#95;table&#95;handling](/ar/operations/settings/settings#query_cache_system_table_handling).

أخيرًا، لا تتم مشاركة إدخالات ذاكرة التخزين المؤقت للاستعلامات بين المستخدمين لأسباب أمنية. على سبيل المثال، يجب ألا يتمكن المستخدم A من تجاوز
row policy على جدول من خلال تشغيل الاستعلام نفسه الذي يشغّله المستخدم B، الذي لا تنطبق عليه مثل هذه السياسة. ومع ذلك، إذا لزم الأمر، يمكن جعل إدخالات ذاكرة التخزين المؤقت
متاحة لمستخدمين آخرين (أي مشتركة) من خلال تمرير الإعداد
[query&#95;cache&#95;share&#95;between&#95;users](/ar/operations/settings/settings#query_cache_share_between_users).

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [التعريف بذاكرة التخزين المؤقت للاستعلامات في ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)