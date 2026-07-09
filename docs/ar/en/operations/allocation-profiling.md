---
description: 'صفحة تعرض تفاصيل تنميط التخصيص في ClickHouse'
sidebar_label: 'تنميط التخصيص'
slug: /operations/allocation-profiling
title: 'تنميط التخصيص'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # تنميط التخصيص
</div>

يستخدم ClickHouse [jemalloc](https://github.com/jemalloc/jemalloc) بوصفه مُخصِّص الذاكرة العام. ويأتي jemalloc مزودًا بأدوات لأخذ عينات التخصيص والتنميط.

يتيح لك ClickHouse وKeeper التحكم في أخذ العينات باستخدام ملفات config، وإعدادات الاستعلام، وأوامر `SYSTEM`، وأوامر الكلمات الأربع (4LW) في Keeper. وهناك عدة طرق لفحص النتائج:

* جمع العينات في `system.trace_log` تحت النوع `JemallocSample` لتحليل كل استعلام على حدة.
* عرض إحصاءات الذاكرة الحية وجلب ملفات تعريف heap عبر [واجهة ويب jemalloc](#jemalloc-web-ui) ‏(26.2+).
* الاستعلام عن ملف تعريف heap الحالي مباشرةً من SQL باستخدام [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) ‏(26.2+).
* تفريغ ملفات تعريف heap إلى القرص وتحليلها باستخدام [`jeprof`](#analyzing-heap-profile-files-with-jeprof).

:::note

ينطبق هذا الدليل على الإصدارات 25.9+.
أما الإصدارات الأقدم، فيُرجى مراجعة [تنميط التخصيص للإصدارات الأقدم من 25.9](/ar/operations/allocation-profiling-old.md).

:::

<div id="sampling-allocations">
  ## أخذ عينات من تخصيصات الذاكرة
</div>

لأخذ عينات من تخصيصات الذاكرة وتحليلها، ابدأ تشغيل ClickHouse/Keeper مع تمكين الإعداد `jemalloc_enable_global_profiler`:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

سيقوم `jemalloc` بأخذ عينات من عمليات تخصيص الذاكرة وتخزين المعلومات داخليًا.

يمكنك أيضًا تفعيل أخذ العينات لكل query باستخدام الإعداد `jemalloc_enable_profiler`.

:::warning تحذير
نظرًا لأن ClickHouse تطبيق يُكثِر من عمليات تخصيص الذاكرة، فقد يترتب على أخذ العينات في `jemalloc` عبء إضافي على الأداء.
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## تخزين عينات jemalloc في `system.trace_log`
</div>

يمكنك تخزين عينات jemalloc في `system.trace_log` تحت النوع `JemallocSample`.
لتمكين ذلك على مستوى النظام، استخدم إعداد `jemalloc_collect_global_profile_samples_in_trace_log`:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning تحذير
نظرًا لأن ClickHouse تطبيق كثيف عمليات التخصيص، فقد يؤدي جمع جميع العينات في system.trace&#95;log إلى حمل مرتفع.
:::

يمكنك أيضًا تفعيل ذلك لكل استعلام باستخدام الإعداد `jemalloc_collect_profile_samples_in_trace_log`.

<div id="example-analyzing-memory-usage-trace-log">
  ### مثال: تحليل استخدام الذاكرة لاستعلام
</div>

أولًا، شغّل استعلامًا مع تفعيل Profiler الخاص بـ jemalloc، ثم اجمع العينات في `system.trace_log`:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
إذا كان ClickHouse قد بدأ باستخدام `jemalloc_enable_global_profiler`، فلن تحتاج إلى تمكين `jemalloc_enable_profiler`.
وينطبق الأمر نفسه على `jemalloc_collect_global_profile_samples_in_trace_log` و `jemalloc_collect_profile_samples_in_trace_log`.
:::

أفرغ `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```

ثم نفِّذ عليه استعلامًا للحصول على استخدام الذاكرة التراكمي مع مرور الوقت:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

اعثر على الوقت الذي بلغ فيه استخدام الذاكرة أعلى مستوى:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

وباستخدام تلك النتيجة، تعرّف على مكدسات التخصيص الأكثر نشاطًا عند الذروة:

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## واجهة ويب jemalloc
</div>

:::note
ينطبق هذا القسم على الإصدارات 26.2+.
:::

يوفّر ClickHouse واجهة ويب مدمجة لعرض إحصاءات ذاكرة jemalloc عند نقطة نهاية HTTP ‏`/jemalloc`.
وتعرض مقاييس الذاكرة المباشرة باستخدام مخططات، بما في ذلك الذاكرة المخصصة، والنشطة، والمقيمة، والمُعيّنة، بالإضافة إلى إحصاءات كل ساحة وكل bin.
كما يمكنك جلب ملفات تعريف heap العامة وملفات تعريف heap لكل استعلام مباشرةً من الواجهة.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    تتضمن واجهة الخادم جميع علامات التبويب: Summary وAllocations وArenas وOperations وGlobal Profiler وQuery Profiler وRaw Output.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    تتوفر واجهة Keeper على منفذ HTTP control. هذا المنفذ **معطّل افتراضيًا** ويجب تمكينه صراحةً عبر تعيين `keeper_server.http_control.port` في إعدادات Keeper:

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    بعد تمكينه، توفّر الواجهة نفس التصورات التي توفّرها واجهة الخادم — Summary وAllocations وArenas وOperations وGlobal Profiler وRaw Output — باستثناء علامة التبويب Query Profiler، لأنها تتطلب SQL و`system.trace_log`.

    :::warning الأمان
    لا يوفّر منفذ HTTP control في Keeper أي authentication على مستوى التطبيق. وعلى عكس واجهة jemalloc في ClickHouse server — حيث تمر جميع استعلامات البيانات عبر معالج SQL HTTP وتتطلب بيانات اعتماد المستخدم/كلمة المرور — فإن نقاط نهاية REST API في Keeper لا تتطلب authentication. وهذا متّسق مع نقاط نهاية HTTP control الأخرى في Keeper (الأوامر، والتخزين، ولوحة المعلومات).

    قيّد الوصول إلى هذا المنفذ باستخدام عناصر تحكم على مستوى الشبكة: اربط Keeper بـ localhost، أو استخدم قواعد جدار حماية، أو ضعه خلف وكيل عكسي مع authentication. وعند عدم ضبط `listen_host`، يستمع Keeper افتراضيًا على localhost فقط.
    :::

    يكشف Keeper أيضًا عن نقاط نهاية REST API للوصول البرمجي:

    * `GET /jemalloc/stats` — مخرجات `malloc_stats_print` الخام
    * `GET /jemalloc/status` — حالة profiling بصيغة JSON (`prof_enabled`, `prof_active`, `thread_active_init`, `lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — يُجري flush لملف تعريف heap مع ترميز الرموز على جانب الخادم، ويُرجع collapsed stacks المناسبة لعرض flame graph (افتراضيًا) أو dump jemalloc الخام
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## جلب ملفات تعريف heap من SQL
</div>

:::note
ينطبق هذا القسم على الإصدارات 26.2+.
:::

يتيح لك جدول النظام `system.jemalloc_profile_text` جلب ملف تعريف heap الحالي لـ jemalloc وعرضه مباشرةً من SQL، من دون الحاجة إلى أدوات خارجية أو إلى تفريغه إلى القرص أولًا.

يحتوي الجدول على عمود واحد:

| العمود | النوع  | الوصف                                                   |
| ------ | ------ | ------------------------------------------------------- |
| `line` | String | سطر من ملف تعريف heap لـ jemalloc مع الرموز المُفسَّرة. |

يمكنك الاستعلام عن الجدول مباشرةً — ولا حاجة إلى تفريغ ملف تعريف heap مسبقًا:

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### تنسيق الإخراج
</div>

يُتحكَّم في تنسيق الإخراج بواسطة الإعداد `jemalloc_profile_text_output_format`، والذي يدعم ثلاث قيم:

* `raw` — heap profile خام كما يُنتجه jemalloc.
* `symbolized` — تنسيق متوافق مع jeprof يتضمن رموز الدوال المضمّنة. وبما أن الرموز مضمّنة بالفعل، يمكن لـ `jeprof` تحليل المخرجات دون الحاجة إلى الملف التنفيذي لـ ClickHouse.
* `collapsed` (الافتراضي) — مكدسات مطوية متوافقة مع FlameGraph، بمكدس واحد في كل سطر مع عدد البايتات.

على سبيل المثال، للحصول على ملف التعريف الخام:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

للحصول على مخرجات تتضمن الرموز المحلولة:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### إعدادات إضافية
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, default: `true`) — ما إذا كان سيتم تضمين الإطارات المضمّنة عند إجراء إسناد الرموز. يؤدي تعطيل هذا الخيار إلى تسريع إسناد الرموز بشكل ملحوظ، لكنه يقلّل الدقة لأن استدعاءات الدوال المضمّنة لن تظهر في مكدسات الاستدعاء. يؤثر هذا فقط في التنسيقين `symbolized` و`collapsed`.
* `jemalloc_profile_text_collapsed_use_count` (Bool, default: `false`) — عند استخدام التنسيق `collapsed`، يُجرى التجميع حسب عدد التخصيصات بدلًا من عدد البايتات.

<div id="example-flamegraph-from-sql">
  ### مثال: إنشاء مخطط لهب من SQL
</div>

نظرًا لأن تنسيق الإخراج الافتراضي هو `collapsed`، يمكنك تمرير الإخراج مباشرةً إلى FlameGraph:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

لإنشاء مخطط لهب استنادًا إلى عدد التخصيصات بدلًا من البايتات:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## تفريغ ملفات تعريف heap إلى القرص
</div>

إذا كنت بحاجة إلى حفظ ملفات تعريف heap كملفات لتحليلها لاحقًا باستخدام `jeprof`، فيمكنك تفريغها إلى القرص.

افتراضيًا، سيُنشأ ملف تعريف heap في `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`، حيث إن `_pid_` هو معرّف العملية (PID) الخاص بـ ClickHouse و`_seqnum_` هو رقم التسلسل العام لملف تعريف heap الحالي.
بالنسبة إلى Keeper، يكون الملف الافتراضي هو `/tmp/jemalloc_keeper._pid_._seqnum_.heap`، ويتبع القواعد نفسها.

لتفريغ ملف التعريف الحالي:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    سيُرجع موقع ملف التعريف المُفرَّغ.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

يمكن تحديد موقع مختلف بإضافة الخيار `prof_prefix` إلى متغير البيئة `MALLOC_CONF`.
على سبيل المثال، إذا كنت تريد إنشاء ملفات التعريف في المجلد `/data` بحيث تكون بادئة اسم الملف `my_current_profile`، فيمكنك تشغيل ClickHouse/Keeper باستخدام متغير البيئة التالي:

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

سيُضاف إلى اسم الملف المُنشأ معرّف PID ورقم تسلسلي.

<div id="analyzing-heap-profile-files-with-jeprof">
  ## تحليل ملفات `ملف تعريف heap` باستخدام `jeprof`
</div>

بعد تفريغ ملفات `ملف تعريف heap` إلى القرص، يمكن تحليلها باستخدام أداة [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) التابعة لـ `jemalloc`. ويمكن تثبيتها بأكثر من طريقة:

* باستخدام مدير الحزم الخاص بالنظام
* باستنساخ [مستودع jemalloc](https://github.com/jemalloc/jemalloc) وتشغيل `autogen.sh` من المجلد الجذر. سيوفر لك ذلك برنامج `jeprof` النصي داخل المجلد `bin`

تتوفر تنسيقات إخراج عديدة. شغّل `jeprof --help` للحصول على القائمة الكاملة بالخيارات.

<div id="symbolized-heap-profiles">
  ### ملفات ملف تعريف heap محلولة الرموز
</div>

بدءًا من الإصدار 26.1+، يُنشئ ClickHouse تلقائيًا ملفات ملف تعريف heap محلولة الرموز عند إجراء flush باستخدام `SYSTEM JEMALLOC FLUSH PROFILE`.
ويحتوي الملف محلول الرموز (ذي الامتداد `.symbolized`) على رموز دوال مضمنة، ويمكن تحليله باستخدام `jeprof` من دون الحاجة إلى الملف التنفيذي لـ ClickHouse.

على سبيل المثال، عند تشغيل:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

سيُرجع ClickHouse مسار ملف التنميط بعد حلّ الرموز (مثلًا، `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`).

يمكنك بعد ذلك تحليله مباشرةً باستخدام `jeprof`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**لا حاجة إلى الملف التنفيذي**: عند استخدام ملفات توصيف الذاكرة المحلولة الرموز (ملفات `.symbolized`)، لا تحتاج إلى تزويد `jeprof` بمسار الملف التنفيذي لـ ClickHouse. وهذا يجعل تحليل ملفات التوصيف أسهل بكثير على أجهزة مختلفة أو بعد تحديث الملف التنفيذي.

:::

إذا كان لديك ملف توصيف قديم للذاكرة غير محلول الرموز، وما زال بإمكانك الوصول إلى الملف التنفيذي لـ ClickHouse، فيمكنك استخدام النهج التقليدي:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

بالنسبة إلى ملفات التعريف غير المزوَّدة بالرموز، يستخدم `jeprof` الأداة `addr2line` لإنشاء تتبعات المكدس، وقد يكون ذلك بطيئًا جدًا.
إذا كان الأمر كذلك، فيُوصى بتثبيت [تنفيذ بديل](https://github.com/gimli-rs/addr2line) لهذه الأداة.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

بدلًا من ذلك، يعمل `llvm-addr2line` بالكفاءة نفسها أيضًا (لكن لاحظ أن `llvm-objdump` غير متوافق مع `jeprof`)

ثم استخدمه لاحقًا على النحو التالي: `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

عند مقارنة ملفَّي تنميط، يمكنك استخدام الوسيط `--base`:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### أمثلة
</div>

باستخدام ملفات التعريف المحلَّلة رمزيًا (مُوصى به):

* أنشئ ملفًا نصيًا، بحيث يُكتب كل إجراء في سطر:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* أنشئ ملف PDF يحتوي على مخطط الاستدعاءات:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

استخدام ملفات التعريف غير المُرمَّزة بالرموز (يتطلب ملفًا تنفيذيًا):

* أنشئ ملفًا نصيًا بحيث تُكتب كل دالة في سطر منفصل:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* أنشئ ملف PDF يحتوي على مخطط الاستدعاءات:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### إنشاء مخطط لهب
</div>

يتيح لك `jeprof` إنشاء مكدسات مطوية لاستخدامها في إنشاء مخططات اللهب.

تحتاج إلى استخدام الوسيطة `--collapsed`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

أو مع ملف تعريف لم تُحلّ رموزه:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

بعد ذلك، يمكنك استخدام العديد من الأدوات المختلفة لتصوّر المكدسات المطوية.

الأداة الأكثر شيوعًا هي [FlameGraph](https://github.com/brendangregg/FlameGraph)، وتتضمن برنامجًا نصيًا يُسمى `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

ومن الأدوات الأخرى الجديرة بالاهتمام [speedscope](https://www.speedscope.app/)، إذ يتيح لك تحليل مكدسات التتبّع المجمّعة بطريقة أكثر تفاعلية.

<div id="additional-options-for-profiler">
  ## خيارات إضافية لأداة التحليل
</div>

يوفّر `jemalloc` العديد من الخيارات المرتبطة بأداة التحليل، ويمكن التحكم فيها من خلال تعديل متغير البيئة `MALLOC_CONF`.
على سبيل المثال، يمكن التحكم في الفاصل الزمني بين عينات التخصيص باستخدام `lg_prof_sample`.
إذا كنت تريد إخراج `ملف تعريف heap` كل N بايت، فيمكنك تفعيل ذلك باستخدام `lg_prof_interval`.

يُنصح بالرجوع إلى [صفحة المرجع](https://jemalloc.net/jemalloc.3.html) الخاصة بـ `jemalloc` للحصول على قائمة كاملة بالخيارات.

<div id="other-resources">
  ## موارد أخرى
</div>

يكشف ClickHouse/Keeper عن المقاييس المرتبطة بـ `jemalloc` بطرق متعددة ومختلفة.

:::warning تحذير
من المهم الانتباه إلى أن هذه المقاييس لا تكون متزامنة مع بعضها بعضًا، وقد تختلف القيم بينها.
:::

<div id="system-table-asynchronous_metrics">
  ### جدول النظام `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[مرجع](/ar/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### جدول النظام `jemalloc_bins`
</div>

يحتوي على معلومات عن تخصيصات الذاكرة التي ينفذها مُخصِّص jemalloc ضمن فئات أحجام (bins) مختلفة، والمجمَّعة من جميع الساحات.

[مرجع](/ar/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### جدول النظام `jemalloc_stats` (26.2+)
</div>

يعرض الناتج الكامل للدالة `malloc_stats_print()` كسلسلة نصية واحدة. وهو مكافئ للأمر `SYSTEM JEMALLOC STATS`.

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

تُعرَض أيضًا جميع المقاييس المتعلقة بـ `jemalloc` من `asynchronous_metrics` عبر نقطة نهاية Prometheus في كلٍّ من ClickHouse وKeeper.

[مرجع](/ar/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### أمر `jmst` ‏4LW في Keeper
</div>

يدعم Keeper أمر `jmst` ‏4LW، الذي يُرجع [إحصاءات أساسية لمُخصِّص الذاكرة](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```