---
description: 'توثيق أداة توصيف الاستعلامات بأخذ العينات في ClickHouse'
sidebar_label: 'توصيف الاستعلامات'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'أداة توصيف الاستعلامات بأخذ العينات'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # أداة توصيف الاستعلام بأخذ العينات
</div>

يشغّل ClickHouse أداة توصيف بأخذ العينات تتيح تحليل تنفيذ الاستعلامات.
وباستخدام أداة التوصيف هذه، يمكنك العثور على إجراءات الشيفرة المصدرية الأكثر استخدامًا أثناء تنفيذ الاستعلام.
كما يمكنك تتبّع وقت CPU والوقت الفعلي المنقضي، بما في ذلك وقت الخمول.

تكون أداة توصيف الاستعلام مفعّلة تلقائيًا في ClickHouse Cloud.
يعثر استعلام المثال التالي على أكثر لقطات المكدس تكرارًا لاستعلام خضع للتوصيف، مع أسماء الدوال بعد تحليلها ومواقعها في الشيفرة المصدرية:

:::tip
استبدل قيمة `query_id` بمعرّف الاستعلام الذي تريد توصيفه.
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    في ClickHouse Cloud، يمكنك الحصول على معرّف الاستعلام بالنقر على **&quot;...&quot;** في أقصى يمين الشريط أعلى جدول نتائج الاستعلام (بجوار مفتاح التبديل بين الجدول/المخطط). يؤدي ذلك إلى فتح قائمة سياقية يمكنك من خلالها النقر على **&quot;Copy query ID&quot;**.

    استخدم `clusterAllReplicas(default, system.trace_log)` للاختيار من جميع العقد في المجموعة:

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="مُدار ذاتيًا">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## استخدام محلّل الاستعلامات في عمليات النشر المُدارة ذاتيًا
</div>

في عمليات النشر المُدارة ذاتيًا، لاستخدام محلّل الاستعلامات اتبع الخطوات التالية:

<VerticalStepper headerLevel="h3">
  ### تثبيت ClickHouse مع معلومات التصحيح

  ثبّت الحزمة `clickhouse-common-static-dbg`:

  1. اتبع التعليمات في الخطوة [&quot;إعداد مستودع Debian&quot;](/ar/install/debian_ubuntu#setup-the-debian-repository)
  2. شغّل `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` لتثبيت الملفات الثنائية المترجمة لـ ClickHouse مع معلومات التصحيح
  3. شغّل `sudo service clickhouse-server start` لبدء تشغيل الخادم
  4. شغّل `clickhouse-client`. سيلتقط الخادم تلقائيًا رموز التصحيح من `clickhouse-common-static-dbg`، ولا تحتاج إلى أي إجراء خاص لتمكينها

  ### التحقق من إعدادات الخادم

  تأكد من إعداد قسم [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) في [ملف تهيئة الخادم](/ar/operations/configuration-files). وهو مُمكَّن افتراضيًا:

  ```xml
  <!-- سجل التتبع. يخزّن تتبعات المكدس التي تجمعها محلّلات الاستعلامات.
       راجع الإعدادين query_profiler_real_time_period_ns و query_profiler_cpu_time_period_ns. -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- يحدد ما إذا كان ينبغي تفريغ السجلات إلى القرص في حال حدوث crash -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  يضبط هذا القسم جدول النظام [trace&#95;log](/ar/operations/system-tables/trace_log) الذي يحتوي على نتائج عمل المحلّل.
  تذكّر أن البيانات في هذا الجدول تكون صالحة فقط أثناء تشغيل الخادم.
  بعد إعادة تشغيل الخادم، لا ينظّف ClickHouse هذا الجدول، وقد تصبح جميع عناوين الذاكرة الافتراضية المخزنة غير صالحة.

  ### تهيئة مؤقتات التحليل

  اضبط الإعدادين [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) أو [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns).
  يمكن استخدام كلا الإعدادين في الوقت نفسه.

  تتيح لك هذه الإعدادات تهيئة مؤقتات المحلّل.
  وبما أنها إعدادات جلسة، يمكنك تعيين تكرار أخذ عينات مختلف للخادم بأكمله، أو لمستخدمين محددين، أو لملفات تعريف المستخدمين، أو لجلسة العمل التفاعلية الخاصة بك، أو لكل query على حدة.

  تكرار أخذ العينات الافتراضي هو عينة واحدة في الثانية، كما أن مؤقتَي CPU والوقت الفعلي مُمكَّنان.
  ويتيح لك هذا التكرار جمع معلومات كافية عن ClickHouse cluster لديك من دون التأثير في أداء الخادم.
  إذا كنت بحاجة إلى تحليل كل query على حدة، فاستخدم تكرار أخذ عينات أعلى.

  ### تحليل جدول النظام `trace_log`

  لتحليل جدول النظام `trace_log`، اسمح باستخدام دوال الاستبطان عبر الإعداد [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions):

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  لأسباب أمنية، تكون دوال الاستبطان معطّلة افتراضيًا
  :::

  استخدم `addressToLine` و`addressToLineWithInlines` و`addressToSymbol` و`demangle` من [دوال الاستبطان](../../sql-reference/functions/introspection.md) للحصول على أسماء الدوال ومواضعها في شيفرة ClickHouse.
  وللحصول على ملف تعريف لبعض query، تحتاج إلى تجميع البيانات من جدول `trace_log`.
  يمكنك تجميع البيانات بحسب الدوال الفردية أو بحسب تتبعات المكدس بالكامل.

  :::tip
  إذا كنت بحاجة إلى تصوّر معلومات `trace_log`، فجرّب [flamegraph](/ar/interfaces/third-party/gui#clickhouse-flamegraph) و[speedscope](https://www.speedscope.app).
  :::
</VerticalStepper>

<div id="flamegraph">
  ## إنشاء مخططات flame graph باستخدام الدالة `flameGraph`
</div>

يوفّر ClickHouse الدالة التجميعية [`flameGraph`](/ar/sql-reference/aggregate-functions/reference/flame_graph)، التي تنشئ flame graph مباشرةً من stack traces المخزَّنة في `trace_log`.
ويكون الناتج مصفوفة من السلاسل النصية بتنسيق متوافق مع [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**الصيغة:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**الوسيطات:**

* `traces` — تتبّع استدعاءات المكدس. [`Array(UInt64)`](/ar/sql-reference/data-types/array).
* `size` — حجم تخصيص لتنميط الذاكرة. [`Int64`](/ar/sql-reference/data-types/int-uint).
* `ptr` — عنوان التخصيص. [`UInt64`](/ar/sql-reference/data-types/int-uint).

عندما تكون قيمة `ptr` غير صفرية، يطابق `flameGraph` بين التخصيصات (`size > 0`) وإلغاء التخصيصات (`size < 0`) التي لها الحجم نفسه والمؤشر نفسه.
لا تُعرض إلا التخصيصات التي لم تُحرَّر.
وتُتجاهل عمليات إلغاء التخصيص غير المطابقة.

<div id="cpu-flame-graph">
  ### مخطط اللهب لـ CPU
</div>

:::note
تتطلب الاستعلامات أدناه أن يكون [flamegraph.pl](https://github.com/brendangregg/FlameGraph) مثبّتًا لديك.

يمكنك القيام بذلك بتشغيل:

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

استبدل `flamegraph.pl` في الاستعلامات التالية بالمسار الذي يوجد فيه الملف `flamegraph.pl` على جهازك
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

نفّذ استعلامك، ثم أنشئ مخطط اللهب:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### الرسم اللهبي للذاكرة — جميع عمليات التخصيص
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

نفِّذ الاستعلام، ثم أنشئ مخطط اللهب:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### مخطط اللهب للذاكرة — عمليات التخصيص التي لم تُحرَّر
</div>

يطابق هذا المتغير عمليات التخصيص بعمليات إلغاء التخصيص حسب المؤشر، ولا يعرض إلا الذاكرة التي لم تُحرَّر أثناء الاستعلام.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

نفّذ الاستعلام التالي لإنشاء مخطط اللهب:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### مخطط الذاكرة اللهبي — التخصيصات النشطة في لحظة زمنية معيّنة
</div>

يتيح لك هذا الأسلوب العثور على ذروة استخدام الذاكرة وتصور ما كان مُخصَّصًا في تلك اللحظة.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### اعثر على استخدام الذاكرة بمرور الوقت
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### اعثر على النقطة الزمنية ذات أعلى استخدام للذاكرة
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### أنشئ مخططًا لهبيًا للتخصيصات النشطة عند تلك اللحظة الزمنية
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### أنشئ مخططًا لهبيًا لعمليات تحرير الذاكرة بعد تلك النقطة الزمنية (لفهم ما الذي تحرر لاحقًا)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## مثال
</div>

مقتطف الشيفرة أدناه:

* يصفّي بيانات `trace_log` حسب معرّف الاستعلام والتاريخ الحالي.
* يجمّع حسب تتبّع المكدس.
* يستخدم دوال الاستبطان للحصول على تقرير يتضمن:
  * أسماء الرموز والدوال المقابلة لها في الشيفرة المصدرية.
  * مواضع هذه الدوال في الشيفرة المصدرية.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```