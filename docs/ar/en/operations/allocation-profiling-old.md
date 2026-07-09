---
description: 'صفحة تفصيلية عن تنميط التخصيص في ClickHouse'
sidebar_label: 'تنميط التخصيص للإصدارات السابقة لـ 25.9'
slug: /operations/allocation-profiling-old
title: 'تنميط التخصيص للإصدارات السابقة لـ 25.9'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # تنميط التخصيص للإصدارات الأقدم من 25.9
</div>

يستخدم ClickHouse ‏[jemalloc](https://github.com/jemalloc/jemalloc) بوصفه `مُخصِّص` العام. ويأتي Jemalloc مزودًا ببعض الأدوات لأخذ عينات من تخصيص الذاكرة وتحليلها.
ولجعل تنميط التخصيص أكثر سهولة، تتوفر أوامر `SYSTEM` إلى جانب أوامر الأحرف الأربعة (4LW) في Keeper.

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## أخذ عينات من تخصيصات الذاكرة وتفريغ ملفات تعريف الكومة
</div>

إذا كنت تريد أخذ عينات من تخصيصات الذاكرة وإنشاء ملفات تعريف لها في `jemalloc`، فيجب تشغيل ClickHouse/Keeper مع تمكين التنميط باستخدام متغير البيئة `MALLOC_CONF`:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

سيأخذ `jemalloc` عينات من تخصيصات الذاكرة ويخزّن المعلومات داخليًا.

يمكنك مطالبة `jemalloc` بتفريغ ملف التعريف الحالي عبر تشغيل:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

افتراضيًا، سيُنشأ ملف ملف تعريف الكومة في `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`، حيث يشير `_pid_` إلى PID الخاص بـ ClickHouse ويشير `_seqnum_` إلى رقم sequence العام لملف ملف تعريف الكومة الحالي.
أما في Keeper، فالملف الافتراضي هو `/tmp/jemalloc_keeper._pid_._seqnum_.heap`، ويخضع للقواعد نفسها.

يمكن تحديد location مختلف بإلحاق الخيار `prof_prefix` بمتغير البيئة `MALLOC_CONF`.
على سبيل المثال، إذا كنت تريد إنشاء ملفات التعريف في المجلد `/data` بحيث تكون بادئة اسم الملف `my_current_profile`، فيمكنك تشغيل ClickHouse/Keeper باستخدام متغير البيئة التالي:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

سيُلحق باسم الملف المُنشأ معرّف العملية PID ورقم تسلسلي.

<div id="analyzing-heap-profiles">
  ## تحليل ملفات تعريف الكومة
</div>

بعد إنشاء ملفات تعريف الكومة، يجب تحليلها.
ولهذا الغرض، يمكن استخدام أداة `jemalloc` المسماة [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). ويمكن تثبيتها بعدة طرق:

* باستخدام مدير الحزم في النظام
* باستنساخ [مستودع jemalloc](https://github.com/jemalloc/jemalloc) وتشغيل `autogen.sh` من المجلد الجذر. وسيؤدي ذلك إلى توفير برنامج `jeprof` النصي داخل مجلد `bin`

:::note
تستخدم `jeprof` الأداة `addr2line` لإنشاء تتبعات المكدس، وقد تكون هذه العملية بطيئة جدًا.
إذا كان الأمر كذلك، فيُنصح بتثبيت [نسخة بديلة](https://github.com/gimli-rs/addr2line) من هذه الأداة.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

هناك العديد من التنسيقات المختلفة التي يمكن توليدها من `ملف تعريف الكومة` باستخدام `jeprof`.
يُوصى بتشغيل `jeprof --help` للحصول على معلومات حول طريقة الاستخدام والخيارات المختلفة التي توفرها الأداة.

وعمومًا، يُستخدم الأمر `jeprof` كما يلي:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

إذا أردت مقارنة عمليات التخصيص التي حدثت بين ملفي تعريف، يمكنك تعيين الوسيطة `base`:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### أمثلة
</div>

* إذا كنت تريد إنشاء ملف نصي يتضمن كل إجراء في سطر منفصل:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* إذا كنت تريد إنشاء ملف PDF يتضمّن مخطط الاستدعاءات:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### إنشاء مخطط اللهب
</div>

يتيح لك `jeprof` إنشاء مكدسات مطوية لاستخدامها في إنشاء مخططات اللهب.

تحتاج إلى استخدام الوسيط `--collapsed`:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

بعد ذلك، يمكنك استخدام العديد من الأدوات المختلفة لعرض المكدسات المطوية بصريًا.

الأداة الأكثر شيوعًا هي [FlameGraph](https://github.com/brendangregg/FlameGraph) وتتضمن برنامجًا نصيًا يُسمى `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

أداة أخرى جديرة بالاهتمام هي [speedscope](https://www.speedscope.app/) التي تتيح لك تحليل مكدسات الاستدعاء المجمّعة بطريقة أكثر تفاعلية.

<div id="controlling-allocation-profiler-during-runtime">
  ## التحكّم في أداة تنميط التخصيص أثناء وقت التشغيل
</div>

إذا بدأ تشغيل ClickHouse/Keeper مع تفعيل أداة التنميط، فسيتم دعم أوامر إضافية لتعطيل/تمكين تنميط التخصيص أثناء وقت التشغيل.
وباستخدام هذه الأوامر، يصبح من الأسهل إجراء التحليل لفترات محددة فقط.

لتعطيل أداة التنميط:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

لتمكين أداة التنميط:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

يمكن أيضًا التحكّم في الحالة الأولية لأداة التنميط من خلال ضبط الخيار `prof_active`، وهو مفعّل افتراضيًا.
على سبيل المثال، إذا كنت لا تريد أخذ عينات من التخصيصات أثناء بدء التشغيل، بل بعده فقط، فيمكنك تمكين أداة التنميط. يمكنك تشغيل ClickHouse/Keeper باستخدام متغيّر البيئة التالي:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

يمكن تفعيل أداة التنميط لاحقًا.

<div id="additional-options-for-profiler">
  ## خيارات إضافية لأداة التنميط
</div>

يوفّر `jemalloc` العديد من الخيارات المختلفة المرتبطة بأداة التنميط. ويمكن التحكم فيها بتعديل متغير البيئة `MALLOC_CONF`.
على سبيل المثال، يمكن التحكم في الفاصل الزمني بين عينات التخصيص باستخدام `lg_prof_sample`.
إذا كنت تريد تفريغ ملف تعريف الكومة كل N بايت، فيمكنك تفعيل ذلك باستخدام `lg_prof_interval`.

يُنصح بالرجوع إلى [المرجع](https://jemalloc.net/jemalloc.3.html) الخاص بـ `jemalloc` للحصول على قائمة كاملة بالخيارات.

<div id="other-resources">
  ## موارد أخرى
</div>

يكشف ClickHouse/Keeper عن مقاييس مرتبطة بـ `jemalloc` بطرق عديدة ومختلفة.

:::warning تحذير
من المهم الانتباه إلى أن هذه المقاييس ليست متزامنة مع بعضها البعض، وقد تختلف القيم فيما بينها.
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

يحتوي على معلومات حول تخصيصات الذاكرة التي أُجريت عبر مُخصِّص jemalloc في فئات الأحجام المختلفة (`bins`) والمجمّعة من جميع الساحات.

[مرجع](/ar/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

تُعرَض أيضًا جميع المقاييس المرتبطة بـ `jemalloc` من `asynchronous_metrics` عبر نقطة نهاية Prometheus في كلٍّ من ClickHouse وKeeper.

[مرجع](/ar/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### أمر 4LW `jmst` في Keeper
</div>

يدعم Keeper أمر 4LW `jmst`، والذي يُرجع [إحصاءات أساسية لمُخصِّص الذاكرة](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```