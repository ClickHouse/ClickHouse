---
description: 'صفحة نظرة عامة على الإعدادات.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'نظرة عامة على الإعدادات'
doc_type: 'مرجع'
---

<div id="overview">
  ## نظرة عامة
</div>

:::note
ملفات تعريف الإعدادات المستندة إلى XML و[ملفات التهيئة](/ar/operations/configuration-files) غير مدعومة حاليًا في ClickHouse Cloud. لتحديد الإعدادات الخاصة بخدمة ClickHouse Cloud، يجب استخدام [ملفات تعريف الإعدادات المُدارة عبر SQL](/ar/operations/access-rights#settings-profiles-management).
:::

فيما يلي المجموعات الرئيسية لإعدادات ClickHouse:

* إعدادات الخادم العامة
* إعدادات الجلسة
* إعدادات الاستعلام
* إعدادات العمليات الخلفية

تُطبَّق الإعدادات العامة افتراضيًا ما لم يتم تجاوزها على مستويات أكثر تحديدًا. ويمكن تحديد إعدادات الجلسة عبر ملفات التعريف وتهيئة المستخدم وأوامر SET. كما يمكن تمرير إعدادات الاستعلام عبر عبارة SETTINGS، وتُطبَّق على كل استعلام على حدة. أما إعدادات العمليات الخلفية فتُطبَّق على Mutations وMerges، وربما على عمليات أخرى تُنفَّذ بشكل غير متزامن في الخلفية.

<div id="see-non-default-settings">
  ## عرض الإعدادات غير الافتراضية
</div>

لعرض الإعدادات التي جرى تغييرها عن قيمتها الافتراضية، يمكنك الاستعلام عن
جدول `system.settings`:

```sql
SELECT name, value FROM system.settings WHERE changed
```

إذا لم يتم تغيير أي إعدادات عن قيمها الافتراضية، فلن يُرجع ClickHouse
أي شيء.

للتحقق من قيمة إعداد معيّن، يمكنك تحديد `name` لذلك
الإعداد في الاستعلام:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

وسيُرجع شيئًا كهذا:

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## قراءات إضافية
</div>

* راجع [إعدادات الخادم العامة](/ar/operations/server-configuration-parameters/settings.md) لمعرفة المزيد حول تهيئة
  خادم ClickHouse لديك على مستوى الخادم العام.
* راجع [إعدادات الجلسة](/ar/operations/settings/settings-query-level.md) لمعرفة المزيد حول تهيئة خادم ClickHouse
  لديك على مستوى الجلسة.
* راجع [التسلسل الهرمي للسياق](/ar/development/architecture.md#context) لمعرفة المزيد حول كيفية معالجة التهيئة في ClickHouse.