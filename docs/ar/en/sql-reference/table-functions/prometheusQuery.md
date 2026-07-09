---
description: 'يُقيِّم استعلام Prometheus باستخدام بيانات من جدول TimeSeries.'
sidebar_label: 'prometheusQuery'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQuery
title: 'prometheusQuery'
doc_type: 'reference'
---

يُقيِّم استعلام Prometheus باستخدام بيانات من جدول TimeSeries.

<div id="syntax">
  ## الصيغة
</div>

```sql
prometheusQuery('db_name', 'time_series_table', 'promql_query', evaluation_time)
prometheusQuery(db_name.time_series_table, 'promql_query', evaluation_time)
prometheusQuery('time_series_table', 'promql_query', evaluation_time)
```

<div id="arguments">
  ## الوسيطات
</div>

* `db_name` - اسم قاعدة البيانات التي يقع فيها جدول TimeSeries.
* `time_series_table` - اسم جدول TimeSeries.
* `promql_query` - استعلام مكتوب وفق [صياغة PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).
* `evaluation_time - الطابع الزمني للتقييم. لتقييم استعلام عند الوقت الحالي، استخدم `now()`كقيمة لـ`evaluation&#95;time&#96;.

<div id="returned_value">
  ## القيمة المُعادة
</div>

يمكن للدالة إرجاع أعمدة مختلفة بحسب نوع نتيجة الاستعلام المُمرَّر إلى المعامل `promql_query`:

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

المحددات اللحظية، ومحددات النطاق، ومطابقات التسميات (`=`, `!=`, `=~`, `!~`)، ومعدِّلات الإزاحة، ومعدِّلات الطابع الزمني @، والاستعلامات الفرعية.

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

**ملاحظة**: تستخدم `histogram_quantile` الاستيفاء الخطي على حاويات المُدرَّج التكراري التقليدي (التي يحدّدها الوسم `le`). لا تزال المُدرَّجات التكرارية الأصلية غير مدعومة، ويجب أن تكون الوسيطة `phi` (مستوى الكوانتايل) حاليًا قيمة `scalar` ثابتة — وتُرفض التعبيرات التي تتغير من خطوة إلى أخرى، مثل `histogram_quantile(time() / 1000, ...)`، مع الخطأ `NOT_IMPLEMENTED`.

<div id="operators">
  ### العوامل
</div>

جميع العوامل الحسابية (`+`, `-`, `*`, `/`, `%`, `^`)، وعوامل المقارنة (`==`, `!=`, `<`, `>`, `<=`, `>=` مع `bool` اختياري)، والعوامل المنطقية الثنائية (`and`, `or`, `unless`)، مع المُعدِّلات `on()`/`ignoring()` و`group_left()`/`group_right()`.

العوامل الأحادية `+` و`-`.

<div id="aggregation-operators">
  ### معاملات التجميع
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — مع إمكانية استخدام المُعدِّلين `by()` أو `without()`.

غير مدعوم بعد: `count_values`.

<div id="example">
  ## مثال
</div>

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```