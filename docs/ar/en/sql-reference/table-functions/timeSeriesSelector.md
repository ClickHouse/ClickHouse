---
description: 'تقرأ سلاسل زمنية من جدول TimeSeries بعد تصفيتها باستخدام مُحدِّد، وبطوابع زمنية ضمن فاصل زمني محدد.'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

تقرأ سلاسل زمنية من جدول TimeSeries بعد تصفيتها باستخدام مُحدِّد، وبطوابع زمنية ضمن فاصل زمني محدد.
تشبه هذه الدالة [مُحدِّدات النطاق](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors)، لكنها تُستخدم أيضًا لتنفيذ [المُحدِّدات اللحظية](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors).

<div id="syntax">
  ## الصيغة
</div>

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

<div id="arguments">
  ## المعاملات
</div>

* `db_name` - اسم قاعدة البيانات التي يوجد فيها جدول TimeSeries.
* `time_series_table` - اسم جدول TimeSeries.
* `instant_query` - محدِّد لحظي مكتوب وفق [صياغة PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors)، من دون المعدِّلين `@` أو `offset`.
* &#96;min&#95;time - الطابع الزمني للبداية، شامل.
* &#96;max&#95;time - الطابع الزمني للنهاية، شامل.

<div id="returned_value">
  ## القيمة المُعادة
</div>

تُرجِع الدالة ثلاثة أعمدة:

* `id` - يحتوي على معرّفات السلاسل الزمنية التي تطابق المُحدِّد المحدد.
* `timestamp` - يحتوي على الطوابع الزمنية.
* `value` - يحتوي على القيم.

لا يوجد ترتيب محدد للبيانات المُعادة.

<div id="example">
  ## مثال
</div>

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```