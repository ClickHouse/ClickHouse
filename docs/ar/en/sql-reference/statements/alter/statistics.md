---
description: 'توثيق إدارة إحصاءات الأعمدة'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'إدارة إحصاءات الأعمدة'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # إدارة إحصاءات الأعمدة
</div>

<CloudNotSupportedBadge />

العمليات التالية متاحة:

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - يضيف وصف الإحصاءات إلى البيانات الوصفية للجداول.

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - يعدّل وصف الإحصاءات في البيانات الوصفية للجداول.

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - يزيل الإحصاءات من البيانات الوصفية للأعمدة المحددة، ويحذف جميع كائنات الإحصاءات في كل الأجزاء لتلك الأعمدة.

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - يحذف جميع كائنات الإحصاءات في كل الأجزاء للأعمدة المحددة. ويمكن إعادة إنشاء كائنات الإحصاءات باستخدام `ALTER TABLE MATERIALIZE STATISTICS`.

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - يعيد إنشاء الإحصاءات للأعمدة. ويُنَفَّذ ذلك على شكل [mutation](../../../sql-reference/statements/alter/index.md#mutations).

الأمران الأولان خفيفان بمعنى أنهما يغيّران البيانات الوصفية فقط أو يزيلان الملفات.

كما أنهما مكرَّران، إذ تُزامَن البيانات الوصفية للإحصاءات عبر ZooKeeper.

<div id="example">
  ## مثال:
</div>

إضافة نوعين من الإحصاءات إلى عمودين:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
الإحصاءات مدعومة فقط للجداول ذات المحرك [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك المتغيرات [المكررة](../../../engines/table-engines/mergetree-family/replication.md)).
:::