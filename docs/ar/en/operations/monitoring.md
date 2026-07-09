---
description: 'يمكنك مراقبة استهلاك موارد الأجهزة وكذلك مقاييس خادم ClickHouse.'
keywords: ['المراقبة', 'observability', 'لوحة المعلومات المتقدمة', 'لوحة المعلومات', 'لوحة معلومات observability']
sidebar_label: 'المراقبة'
sidebar_position: 45
slug: /operations/monitoring
title: 'المراقبة'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # المراقبة
</div>

:::note
يمكن الوصول إلى بيانات المراقبة الموضحة في هذا الدليل في ClickHouse Cloud. وإلى جانب عرضها عبر لوحة المعلومات المضمنة الموضحة أدناه، يمكن أيضًا عرض مقاييس الأداء الأساسية والمتقدمة مباشرةً في وحدة تحكم الخدمة الرئيسية.
:::

يمكنك مراقبة:

* استخدام موارد الأجهزة.
* مقاييس خادم ClickHouse.

<div id="built-in-advanced-observability-dashboard">
  ## لوحة معلومات observability المتقدمة المضمّنة
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="لقطة شاشة بتاريخ 2023-11-12 الساعة 6 08 58 مساءً" size="md" />

يأتي ClickHouse مزوّدًا بميزة لوحة معلومات observability متقدمة مضمّنة، ويمكن الوصول إليها عبر `$HOST:$PORT/dashboard` (ويتطلب ذلك اسم مستخدم وكلمة مرور)، وتعرض المقاييس التالية:

* الاستعلامات/الثانية
* استخدام CPU (الأنوية)
* الاستعلامات قيد التشغيل
* عمليات الدمج قيد التشغيل
* البايتات المقروءة/الثانية
* انتظار IO
* انتظار CPU
* استخدام CPU لنظام التشغيل (userspace)
* استخدام CPU لنظام التشغيل (kernel)
* القراءة من disk
* القراءة من filesystem
* الذاكرة (المتعقبة)
* الصفوف المُدرجة/الثانية
* إجمالي أجزاء MergeTree
* الحد الأقصى لعدد الأجزاء لكل partition

<div id="resource-utilization">
  ## استهلاك الموارد
</div>

يراقب ClickHouse أيضًا حالة موارد الأجهزة بنفسه، مثل:

* الحمل ودرجة حرارة المعالجات.
* استخدام نظام التخزين وذاكرة RAM والشبكة.

تُجمع هذه البيانات في جدول `system.asynchronous_metric_log`.

<div id="clickhouse-server-metrics">
  ## مقاييس خادم ClickHouse
</div>

يحتوي خادم ClickHouse على أدوات مضمّنة لمراقبة حالته تلقائيًا.

لتتبّع أحداث الخادم، استخدم سجلات الخادم. راجع قسم [logger](../operations/server-configuration-parameters/settings.md#logger) في ملف التكوين.

يجمع ClickHouse ما يلي:

* مقاييس مختلفة توضّح كيفية استخدام الخادم للموارد الحاسوبية.
* إحصاءات عامة عن معالجة الاستعلامات.

يمكنك العثور على المقاييس في الجداول [system.metrics](/ar/operations/system-tables/metrics) و[system.events](/ar/operations/system-tables/events) و[system.asynchronous&#95;metrics](/ar/operations/system-tables/asynchronous_metrics).

يمكنك تهيئة ClickHouse لتصدير المقاييس إلى [Graphite](https://github.com/graphite-project). راجع [قسم Graphite](../operations/server-configuration-parameters/settings.md#graphite) في ملف تكوين خادم ClickHouse. قبل تهيئة تصدير المقاييس، ينبغي إعداد Graphite باتباع [دليلهم](https://graphite.readthedocs.io/en/latest/install.html) الرسمي.

يمكنك تهيئة ClickHouse لتصدير المقاييس إلى [Prometheus](https://prometheus.io). راجع [قسم Prometheus](../operations/server-configuration-parameters/settings.md#prometheus) في ملف تكوين خادم ClickHouse. قبل تهيئة تصدير المقاييس، ينبغي إعداد Prometheus باتباع [دليلهم](https://prometheus.io/docs/prometheus/latest/installation/) الرسمي.

بالإضافة إلى ذلك، يمكنك مراقبة مدى توفّر الخادم عبر واجهة برمجة تطبيقات HTTP. أرسل طلب `HTTP GET` إلى `/ping`. إذا كان الخادم متاحًا، فسيستجيب بـ `200 OK`.

لمراقبة الخوادم في تكوين عنقودي، ينبغي ضبط المعامل [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) واستخدام مورد HTTP ‏`/replicas_status`. يعيد الطلب إلى `/replicas_status` القيمة `200 OK` إذا كانت النسخة المتماثلة متاحة ولم تتأخر عن النسخ المتماثلة الأخرى. وإذا كانت النسخة المتماثلة متأخرة، فسيُرجع `503 HTTP_SERVICE_UNAVAILABLE` مع معلومات عن الفجوة.