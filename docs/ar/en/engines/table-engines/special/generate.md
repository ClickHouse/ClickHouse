---
description: 'ينتج محرك الجدول GenerateRandom بيانات عشوائية استنادًا إلى مخطط الجدول المحدد.'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'محرك الجدول GenerateRandom'
doc_type: 'مرجع'
---

ينتج محرك الجدول GenerateRandom بيانات عشوائية استنادًا إلى مخطط الجدول المحدد.

أمثلة الاستخدام:

* استخدمه في الاختبارات لملء جدول كبير ببيانات يمكن إعادة إنتاجها.
* أنشئ مدخلات عشوائية لاختبارات التشويش.

<div id="usage-in-clickhouse-server">
  ## الاستخدام في خادم ClickHouse
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

تحدّد المعلَمتان `max_array_length` و`max_string_length` الحد الأقصى لطول جميع
أعمدة Array أو Map والسلاسل النصية، على التوالي، في البيانات المُولَّدة.

لا يدعم محرك الجدول Generate سوى استعلامات `SELECT`.

وهو يدعم جميع [أنواع البيانات](../../../sql-reference/data-types/index.md) التي يمكن تخزينها في جدول، باستثناء `AggregateFunction`.

<div id="example">
  ## مثال
</div>

**1.** قم بإنشاء الجدول `generate_engine_table`:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** نفّذ استعلامًا على البيانات:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## تفاصيل التنفيذ
</div>

* غير مدعوم:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * الفهارس
  * النسخ المتماثل