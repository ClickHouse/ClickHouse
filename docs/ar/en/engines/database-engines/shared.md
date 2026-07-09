---
description: 'صفحة تشرح محرك قاعدة البيانات `Shared`، المتوفر في ClickHouse Cloud'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'مرجع'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # محرك قاعدة البيانات Shared
</div>

يعمل محرك قاعدة البيانات `Shared` بالتكامل مع Shared Catalog لإدارة قواعد البيانات التي تستخدم جداولها محركات جداول عديمة الحالة مثل [`SharedMergeTree`](/ar/cloud/reference/shared-merge-tree).
ولا تكتب محركات الجداول هذه حالة دائمة على القرص، وهي متوافقة مع بيئات حوسبة ديناميكية.

يلغي محرك قاعدة البيانات `Shared` في Cloud الحاجة إلى الأقراص المحلية.
وهو محرك يعمل بالكامل في الذاكرة، ولا يتطلب سوى CPU والذاكرة.

<div id="how-it-works">
  ## كيف يعمل؟
</div>

يقوم محرك قاعدة البيانات `Shared` بتخزين جميع تعريفات قواعد البيانات والجداول في Shared Catalog مركزي يستند إلى Keeper. وبدلًا من الكتابة إلى القرص المحلي، يحتفظ بحالة عامة موحّدة واحدة مُرقّمة بالإصدارات ومشتركة بين جميع عُقد الحوسبة.

تتعقّب كل عقدة آخر إصدار مُطبَّق فقط، وعند بدء التشغيل تجلب أحدث حالة من دون الحاجة إلى ملفات محلية أو إعداد يدوي.

<div id="syntax">
  ## الصيغة
</div>

بالنسبة إلى المستخدمين النهائيين، لا يتطلب استخدام Shared Catalog ومحرك قاعدة البيانات Shared أي إعدادات إضافية. وتبقى عملية إنشاء قاعدة البيانات كما هي دائمًا:

```sql
CREATE DATABASE my_database;
```

تُسنِد ClickHouse Cloud تلقائيًا محرك قاعدة البيانات محرك قاعدة البيانات Shared إلى قواعد البيانات. وأي جداول تُنشأ داخل قاعدة بيانات من هذا النوع باستخدام محركات عديمة الحالة ستستفيد تلقائيًا من إمكانات النسخ المتماثل والتنسيق التي يوفّرها Shared Catalog.

:::tip
لمزيد من المعلومات عن Shared Catalog وفوائده، راجع [&quot;Shared catalog and shared database engine&quot;](/ar/cloud/reference/shared-catalog) في قسم مرجع Cloud.
:::