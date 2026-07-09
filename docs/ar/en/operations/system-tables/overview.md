---
description: 'نظرة عامة على ماهية جداول النظام وسبب أهميتها.'
keywords: ['جداول النظام', 'نظرة عامة']
sidebar_label: 'نظرة عامة'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'نظرة عامة على جداول النظام'
doc_type: 'reference'
---

<div id="system-tables-introduction">
  ## نظرة عامة على جداول النظام
</div>

توفّر جداول النظام معلومات حول:

* حالات الخادم وعملياته وبيئته.
* العمليات الداخلية للخادم.
* الخيارات المستخدمة عند بناء الملف التنفيذي لـ ClickHouse.

جداول النظام:

* موجودة في قاعدة البيانات `system`.
* متاحة لقراءة البيانات فقط.
* لا يمكن حذفها أو تعديلها، لكن يمكن فصلها.

تخزّن معظم جداول النظام بياناتها في RAM. وينشئ خادم ClickHouse جداول النظام هذه عند بدء التشغيل.

وعلى خلاف جداول النظام الأخرى، فإن جداول سجلات النظام [metric&#95;log](../../operations/system-tables/metric_log.md) و[query&#95;log](../../operations/system-tables/query_log.md) و[query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md) و[trace&#95;log](../../operations/system-tables/trace_log.md) و[part&#95;log](../../operations/system-tables/part_log.md) و[crash&#95;log](../../operations/system-tables/crash_log.md) و[text&#95;log](../../operations/system-tables/text_log.md) و[backup&#95;log](../../operations/system-tables/backup_log.md) تعمل باستخدام محرك الجداول [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) وتخزّن بياناتها في نظام الملفات افتراضيًا. إذا أزلت جدولًا من نظام الملفات، فإن خادم ClickHouse ينشئ جدولًا فارغًا من جديد عند عملية كتابة البيانات التالية. وإذا تغيّر مخطط جدول النظام في إصدار جديد، فسيعيد ClickHouse تسمية الجدول الحالي وينشئ جدولًا جديدًا.

يمكن تخصيص جداول سجلات النظام من خلال إنشاء ملف config بالاسم نفسه للجدول ضمن `/etc/clickhouse-server/config.d/`، أو عبر ضبط العناصر المقابلة في `/etc/clickhouse-server/config.xml`. والعناصر التي يمكن تخصيصها هي:

* `database`: قاعدة البيانات التي ينتمي إليها جدول سجل النظام. هذا الخيار مهمل الآن. جميع جداول سجلات النظام موجودة ضمن قاعدة البيانات `system`.
* `table`: الجدول الذي تُدرج فيه البيانات.
* `partition_by`: حدّد تعبير [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
* `ttl`: حدّد تعبير [TTL](../../sql-reference/statements/alter/ttl.md) للجدول.
* `flush_interval_milliseconds`: الفاصل الزمني لتفريغ البيانات إلى القرص.
* `engine`: وفّر تعبير engine كاملًا (يبدأ بـ `ENGINE =` ) مع المعاملات. يتعارض هذا الخيار مع `partition_by` و`ttl`. وإذا جرى تعيينها معًا، فسيرفع الخادم استثناءً ثم يخرج.

مثال:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

افتراضيًا، يكون نمو الجدول غير محدود. للتحكم في حجم الجدول، يمكنك استخدام إعدادات [TTL](/ar/sql-reference/statements/alter/ttl) لإزالة السجلات القديمة. كما يمكنك استخدام ميزة التقسيم في الجداول التي تستخدم المحرك `MergeTree`.

<div id="system-tables-sources-of-system-metrics">
  ## مصادر مقاييس النظام
</div>

لجمع مقاييس النظام، يستخدم خادم ClickHouse ما يلي:

* القدرة `CAP_NET_ADMIN`.
* [procfs](https://en.wikipedia.org/wiki/Procfs) (في Linux فقط).

**procfs**

إذا لم تتوفر لدى خادم ClickHouse القدرة `CAP_NET_ADMIN`، فسيحاول تلقائيًا اللجوء إلى `ProcfsMetricsProvider`. ويتيح `ProcfsMetricsProvider` جمع مقاييس النظام على مستوى كل استعلام (لـ CPU و I/O).

إذا كان procfs مدعومًا ومفعّلًا على النظام، فإن خادم ClickHouse يجمع المقاييس التالية:

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
يكون `OSIOWaitMicroseconds` معطّلًا افتراضيًا في نوى Linux بدءًا من الإصدار 5.14.x.
يمكنك تمكينه باستخدام `sudo sysctl kernel.task_delayacct=1` أو بإنشاء ملف `.conf` في `/etc/sysctl.d/` يتضمن `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## جداول النظام في ClickHouse Cloud
</div>

في ClickHouse Cloud، توفّر جداول النظام معلومات مهمة عن حالة الخدمة وأدائها، تمامًا كما هو الحال في البيئات المُدارة ذاتيًا. تعمل بعض جداول النظام على مستوى العنقود بأكمله، ولا سيّما تلك التي تستمد بياناتها من عُقد Keeper التي تدير البيانات الوصفية الموزعة. وتعكس هذه الجداول الحالة العامة للعنقود، لذا ينبغي أن تكون متسقة عند الاستعلام عنها من العُقد الفردية. على سبيل المثال، يجب أن يكون [`parts`](/ar/operations/system-tables/parts) متسقًا بغض النظر عن العقدة التي يُجرى الاستعلام منها:

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

وعلى النقيض من ذلك، تكون بعض جداول النظام الأخرى خاصة بكل عقدة، مثل تلك الموجودة في الذاكرة أو التي تحتفظ ببياناتها باستخدام محرك الجدول MergeTree. وهذا شائع في بيانات مثل السجلات والمقاييس. ويضمن هذا الاحتفاظ بقاء البيانات التاريخية متاحةً للتحليل. ومع ذلك، فإن هذه الجداول الخاصة بالعقدة تكون بطبيعتها فريدة لكل عقدة.

بوجه عام، يمكن تطبيق القواعد التالية عند تحديد ما إذا كان جدول نظام ما خاصًا بالعقدة:

* جداول النظام التي تحمل اللاحقة `_log`.
* جداول النظام التي تعرض المقاييس، مثل `metrics` و`asynchronous_metrics` و`events`.
* جداول النظام التي تعرض العمليات الجارية، مثل `processes` و`merges`.

إضافةً إلى ذلك، قد تُنشأ إصدارات جديدة من جداول النظام نتيجةً للترقيات أو للتغييرات في المخطط الخاص بها. وتُسمّى هذه الإصدارات باستخدام لاحقة رقمية.

على سبيل المثال، تأمل جداول `system.query_log` التي تحتوي على صف لكل query تُنفِّذه العقدة:

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### الاستعلام عن عدة إصدارات
</div>

يمكننا الاستعلام في هذه الجداول باستخدام الدالة [`merge`](/ar/sql-reference/table-functions/merge). على سبيل المثال، يحدّد الاستعلام أدناه أحدث استعلام وُجِّه إلى العقدة المستهدفة في كل جدول `query_log`:

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note لا تعتمد على اللاحقة الرقمية لتحديد الترتيب
مع أن اللاحقة الرقمية في الجداول قد توحي بترتيب البيانات، فلا ينبغي الاعتماد عليها مطلقًا. لذلك، استخدم دائمًا دالة الجدول merge مع عامل تصفية حسب التاريخ عند تحديد نطاقات تاريخية معيّنة.
:::

ومن المهم ملاحظة أن هذه الجداول تظل **محلية على كل عقدة**.

<div id="querying-across-nodes">
  ### الاستعلام عن البيانات عبر العُقد
</div>

للحصول على رؤية شاملة للعنقود بأكمله، يمكن للمستخدمين الاستفادة من الدالة [`clusterAllReplicas`](/ar/sql-reference/table-functions/cluster) بالاقتران مع الدالة `merge`. تتيح الدالة `clusterAllReplicas` إجراء الاستعلام على جداول النظام عبر جميع النسخ المتماثلة ضمن العنقود &quot;default&quot;، مع تجميع البيانات الخاصة بكل عقدة في نتيجة موحّدة. وعند دمجها مع الدالة `merge`، يمكن استخدام ذلك لاستهداف جميع بيانات النظام الخاصة بجدول معيّن في عنقود.

تكتسب هذه المقاربة أهمية خاصة عند مراقبة العمليات على مستوى العنقود واستكشاف أخطائها، بما يضمن تمكّن المستخدمين من تحليل سلامة وأداء نشر ClickHouse Cloud بفعالية.

:::note
يوفّر ClickHouse Cloud عناقيد تضم عدة نسخ متماثلة لتحقيق التكرار الاحتياطي والتبديل الاحتياطي. ويتيح ذلك ميزاته مثل التوسّع التلقائي الديناميكي والترقيات دون توقف. وفي لحظة زمنية معيّنة، قد تكون هناك عُقد جديدة قيد الإضافة إلى العنقود أو تُزال منه. لتخطي هذه العقد، أضف `SETTINGS skip_unavailable_shards = 1` إلى الاستعلامات التي تستخدم `clusterAllReplicas` كما هو موضح أدناه.
:::

على سبيل المثال، لاحظ الفرق عند الاستعلام عن جدول `query_log` — وهو غالبًا ما يكون أساسيًا للتحليل.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### الاستعلام عن العُقد والإصدارات
</div>

بسبب إدارة الإصدارات في جداول النظام، لا يزال هذا لا يمثّل كامل البيانات في العنقود. عند دمج ما سبق مع الدالة `merge`، نحصل على نتيجة دقيقة للنطاق الزمني المحدد:

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## محتوى مرتبط
</div>

* مدونة: [جداول النظام ونظرة على البنية الداخلية لـ ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* مدونة: [استعلامات المراقبة الأساسية - الجزء 1 - استعلامات INSERT](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* مدونة: [استعلامات المراقبة الأساسية - الجزء 2 - استعلامات SELECT](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)