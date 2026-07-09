---
description: 'يُقيِّم استعلام Prometheus باستخدام بيانات من جدول TimeSeries.'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

يُقيِّم استعلام Prometheus باستخدام بيانات من جدول TimeSeries عبر نطاق من أوقات التقييم.

<div id="syntax">
  ## الصياغة
</div>

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

<div id="arguments">
  ## الوسائط
</div>

* `db_name` - اسم قاعدة البيانات التي يوجد فيها جدول TimeSeries.
* `time_series_table` - اسم جدول TimeSeries.
* `promql_query` - استعلام مكتوب بصيغة [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).
* `start_time` - وقت بداية نطاق التقييم.
* `end_time` - وقت نهاية نطاق التقييم.
* `step` - الخطوة المستخدمة لتكرار وقت التقييم من `start_time` إلى `end_time` (بما في ذلك القيمتان).

<div id="returned_value">
  ## القيمة المُعادة
</div>

يمكن أن تُرجِع الدالة أعمدة مختلفة بحسب نوع نتيجة الاستعلام المُمرَّر إلى المعلمة `promql_query`:

| نوع النتيجة | أعمدة النتيجة                                                                             | مثال                                                |
| ----------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector      | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix      | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar      | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string      | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## الميزات المدعومة في PromQL
</div>

<div id="selectors">
  ### المحددات
</div>

المحددات اللحظية، ومحددات النطاق، ومطابقات الوسوم (`=`, `!=`, `=~`, `!~`)، ومُعدِّلات الإزاحة، ومُعدِّلات الطابع الزمني `@`، والاستعلامات الفرعية.

<div id="functions">
  ### الدوال
</div>

| الفئة          | الدوال                                                                                           |
| -------------- | ------------------------------------------------------------------------------------------------ |
| النطاق         | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| الرياضيات      | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| حساب المثلثات  | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| DateTime       | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| النوع          | `scalar`, `vector`                                                                               |
| مُدرَّج تكراري | `histogram_quantile`                                                                             |
| أخرى           | `time`, `pi`                                                                                     |

**ملاحظة**: تستخدم `histogram_quantile` الاستيفاء الخطي على سلال المُدرَّج التكراري التقليدية (التي يحدّدها الوسم `le`). لا تزال المُدرَّجات التكرارية الأصلية غير مدعومة، ويجب أن تكون الوسيطة `phi` (مستوى الكوانتايل) حاليًا قيمة `scalar` ثابتة — وتُرفض التعبيرات التي تتغير في كل خطوة، مثل `histogram_quantile(time() / 1000, ...)`، مع ظهور الخطأ `NOT_IMPLEMENTED`.

<div id="operators">
  ### عوامل التشغيل
</div>

جميع عوامل التشغيل الثنائية الحسابية (`+`, `-`, `*`, `/`, `%`, `^`)، وعوامل تشغيل المقارنة (`==`, `!=`, `<`, `>`, `<=`, `>=` مع `bool` اختياريًا)، وعوامل التشغيل المنطقية (`and`, `or`, `unless`)، مع المعدِّلات `on()`/`ignoring()` و`group_left()`/`group_right()`.

عوامل التشغيل الأحادية `+` و`-`.

<div id="aggregation-operators">
  ### عوامل تشغيل التجميع
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — مع المُعدِّلين الاختياريين `by()` أو `without()`.

غير مدعوم بعد: `count_values`.

<div id="example">
  ## مثال
</div>

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```