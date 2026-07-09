---
description: 'دليل لاستخدام OpenTelemetry في التتبّع الموزّع وجمع المقاييس
  في ClickHouse'
sidebar_label: 'تتبّع ClickHouse باستخدام OpenTelemetry'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'تتبّع ClickHouse باستخدام OpenTelemetry'
doc_type: 'guide'
---

يُعد [OpenTelemetry](https://opentelemetry.io/) معيارًا مفتوحًا لجمع التتبعات والمقاييس من التطبيقات الموزعة. ويوفّر ClickHouse دعمًا جزئيًا لـ OpenTelemetry.

<div id="supplying-trace-context-to-clickhouse">
  ## تزويد ClickHouse بسياق التتبّع
</div>

يقبل ClickHouse رؤوس HTTP الخاصة بسياق التتبّع، كما هو موضح في [توصية W3C](https://www.w3.org/TR/trace-context/). كما يقبل سياق التتبّع عبر البروتوكول الأصلي المستخدم للتواصل بين خوادم ClickHouse أو بين العميل والخادم. ولأغراض الاختبار اليدوي، يمكن تمرير رؤوس سياق التتبّع المتوافقة مع توصية Trace Context إلى `clickhouse-client` باستخدام الخيارين `--opentelemetry-traceparent` و `--opentelemetry-tracestate`.

إذا لم يتم تمرير سياق تتبّع أب، أو كان سياق التتبّع الممرَّر لا يتوافق مع معيار W3C المذكور أعلاه، فيمكن لـ ClickHouse بدء تتبّع جديد باحتمالية يتحكم فيها الإعداد [opentelemetry&#95;start&#95;trace&#95;probability](/ar/operations/settings/settings#opentelemetry_start_trace_probability).

<div id="propagating-the-trace-context">
  ## تمرير سياق التتبّع
</div>

يُمرَّر سياق التتبّع إلى الخدمات اللاحقة في الحالات التالية:

* الاستعلامات المُرسلة إلى خوادم ClickHouse البعيدة، مثل عند استخدام محرك الجدول [Distributed](../engines/table-engines/special/distributed.md).

* دالة الجدول [url](../sql-reference/table-functions/url.md). تُرسَل معلومات سياق التتبّع في رؤوس HTTP.

<div id="tracing-clickhouse-keeper-requests">
  ## تتبّع طلبات ClickHouse Keeper
</div>

يدعم ClickHouse تتبّع OpenTelemetry لطلبات [ClickHouse Keeper](../guides/sre/keeper/index.md) (وهي خدمة تنسيق متوافقة مع ZooKeeper). تتيح هذه الميزة رؤية تفصيلية للمراحل التي تمر بها عمليات Keeper، بدءًا من إرسال الطلب من العميل وحتى المعالجة على جهة الخادم.

<div id="enabling-keeper-tracing">
  ### تمكين تتبّع Keeper
</div>

لتمكين تتبّع طلبات Keeper، اضبط الإعدادات التالية في إعدادات عميل ZooKeeper/Keeper:

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### أنواع span الخاصة بـ Keeper
</div>

عند تمكين التتبّع، ينشئ ClickHouse span لكلٍ من عمليات Keeper على جهة العميل وعلى جهة الخادم:

**span على جهة العميل:**

* `zookeeper.create` — إنشاء عقدة جديدة
* `zookeeper.get` — جلب بيانات العقدة
* `zookeeper.set` — تعيين بيانات العقدة
* `zookeeper.remove` — إزالة عقدة
* `zookeeper.list` — سرد العقد الفرعية
* `zookeeper.exists` — التحقّق من وجود العقدة
* `zookeeper.multi` — تنفيذ عدة عمليات بشكل ذري
* `zookeeper.client.requests_queue` — الوقت المستغرَق في انتظار الطلبات في الطابور قبل إرسالها

**span على جهة الخادم (Keeper):**

* `keeper.receive_request` — استلام الطلب من العميل وتحليله
* `keeper.dispatcher.requests_queue` — انتظار الطلبات في طابور الموزّع
* `keeper.write.pre_commit` — المعالجة المسبقة لطلبات الكتابة قبل Raft commit
* `keeper.write.commit` — معالجة طلبات الكتابة بعد Raft commit
* `keeper.read.wait_for_write` — انتظار طلبات القراءة لعمليات الكتابة التابعة
* `keeper.read.process` — معالجة طلبات القراءة
* `keeper.dispatcher.responses_queue` — انتظار الاستجابات في طابور الموزّع
* `keeper.send_response` — إرسال الاستجابة إلى العميل

<div id="sampling-and-performance">
  ### أخذ العينات والأداء
</div>

لإدارة العبء الإضافي للتتبّع، ينفّذ Keeper أخذ عينات ديناميكيًا. ويُضبَط معدل أخذ العينات تلقائيًا بين 1/10,000 و1/10 بناءً على حجم الطلب. وتُسجَّل مدد جميع الطلبات (التي أُخذت منها عينات والتي لم تُؤخذ) ضمن مقاييس المُدرَّج التكراري لمراقبة الأداء.

<div id="tracing-the-clickhouse-itself">
  ## تتبّع ClickHouse نفسه
</div>

ينشئ ClickHouse `trace spans` لكل استعلام، وكذلك لبعض مراحل تنفيذ الاستعلام، مثل تخطيط الاستعلام أو الاستعلامات الموزعة.

ولكي تكون معلومات التتبّع مفيدة، يجب تصديرها إلى نظام مراقبة يدعم OpenTelemetry، مثل [Jaeger](https://jaegertracing.io/) أو [Prometheus](https://prometheus.io/). ويتجنب ClickHouse الاعتماد على نظام مراقبة بعينه، ويكتفي بدلًا من ذلك بإتاحة بيانات التتبّع عبر جدول نظام فقط. وتُخزَّن معلومات OpenTelemetry trace span [التي يتطلبها المعيار](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) في جدول [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md).

يجب تمكين هذا الجدول في تهيئة الخادم. راجع العنصر `opentelemetry_span_log` في ملف التهيئة الافتراضي `config.xml`. وهو مُمكَّن افتراضيًا.

تُحفَظ الوسوم أو السمات في مصفوفتين متوازيتين تحتويان على المفاتيح والقيم. استخدم [ARRAY JOIN](../sql-reference/statements/select/array-join.md) للعمل معها.

<div id="log-query-settings">
  ## إعدادات سجل الاستعلام
</div>

يتيح الإعداد [log&#95;query&#95;settings](settings/settings.md) تسجيل التغييرات على إعدادات الاستعلام أثناء تنفيذ الاستعلام. وعند تمكينه، تُسجَّل أي تعديلات تُجرى على إعدادات الاستعلام في سجل الـ span الخاص بـ OpenTelemetry. وتُعد هذه الميزة مفيدةً بشكل خاص في بيئات الإنتاج لتتبّع تغييرات الإعدادات التي قد تؤثر في أداء الاستعلام.

<div id="integration-with-monitoring-systems">
  ## التكامل مع أنظمة المراقبة
</div>

في الوقت الحالي، لا توجد أداة جاهزة يمكنها تصدير بيانات التتبّع من ClickHouse إلى نظام مراقبة.

لأغراض الاختبار، يمكن إعداد التصدير باستخدام عرض متجسّد مع محرّك [URL](../engines/table-engines/special/url.md) على جدول [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md)، بحيث يرسل بيانات السجل الواردة إلى endpoint عبر HTTP لمجمّع تتبّع. على سبيل المثال، لإرسال الحد الأدنى من بيانات span إلى مثيل Zipkin يعمل على `http://localhost:9411`، بتنسيق Zipkin v2 JSON:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

في حال حدوث أي أخطاء، فسيُفقد بصمت الجزء من بيانات السجل الذي حدث فيه الخطأ. تحقّق من سجل الخادم بحثًا عن رسائل الخطأ إذا لم تصل البيانات.

<div id="related-content">
  ## محتوى ذي صلة
</div>

* مدونة: [بناء حل أوبزرفابيليتي باستخدام ClickHouse - الجزء 2 - التتبعات](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)