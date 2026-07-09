---
description: 'توثيق لعبارة APPLY PATCHES للتحديثات خفيفة الوزن'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'APPLY PATCHES للتحديثات خفيفة الوزن'
doc_type: 'مرجع'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

يُفعِّل هذا الأمر يدويًا التجسيد المادي الفعلي لأجزاء التصحيح التي تُنشئها عبارات [تحديث `UPDATE` خفيف](/ar/sql-reference/statements/update). وهو يفرض تطبيق التصحيحات المعلّقة على أجزاء البيانات من خلال إعادة كتابة الأعمدة المتأثرة فقط.

:::note

* لا يعمل إلا مع الجداول ضمن عائلة [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك الجداول [المُكرَّرة](../../../engines/table-engines/mergetree-family/replication.md)).
* هذه عملية mutation وتُنفَّذ بشكل غير متزامن في الخلفية.
  :::

<div id="when-to-use">
  ## متى تستخدم APPLY PATCHES
</div>

:::tip
بوجه عام، لا ينبغي أن تحتاج إلى استخدام `APPLY PATCHES`
:::

تُطبَّق أجزاء التصحيح عادةً تلقائيًا أثناء عمليات الدمج عند تفعيل الإعداد [`apply_patches_on_merge`](/ar/operations/settings/merge-tree-settings#apply_patches_on_merge) (وهو الإعداد الافتراضي). ومع ذلك، قد ترغب في تشغيل تطبيق التصحيحات يدويًا في الحالات التالية:

* لتقليل التكلفة الإضافية الناتجة عن تطبيق التصحيحات أثناء استعلامات `SELECT`
* لدمج عدة أجزاء تصحيح قبل أن تتراكم
* لإعداد البيانات للنسخ الاحتياطي أو التصدير بعد تثبيت التصحيحات فعليًا مسبقًا
* عندما يكون `apply_patches_on_merge` معطّلًا وتريد التحكم في توقيت تطبيق التصحيحات

<div id="examples">
  ## أمثلة
</div>

طبّق جميع التصحيحات المعلّقة على أحد الجداول:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

طبّق التصحيحات على تجزئة محددة فقط:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

الدمج مع العمليات الأخرى:

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## مراقبة تطبيق التصحيح
</div>

يمكنك متابعة تقدّم تطبيق التصحيح باستخدام جدول [`system.mutations`](/ar/operations/system-tables/mutations):

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## انظر أيضًا
</div>

* [التحديث `UPDATE` الخفيف](/ar/sql-reference/statements/update) - أنشئ أجزاء التصحيح باستخدام تحديث خفيف الوزن
* [إعداد `apply_patches_on_merge`](/ar/operations/settings/merge-tree-settings#apply_patches_on_merge) - تحكّم في التطبيق التلقائي للتصحيحات أثناء عمليات الدمج