---
description: 'توثيق لعبارات DDL الموزعة'
sidebar_label: 'DDL الموزعة'
sidebar_position: 3
slug: /sql-reference/distributed-ddl
title: 'استعلامات DDL الموزعة (عبارة ON CLUSTER)'
doc_type: 'reference'
---

افتراضيًا، لا تؤثر استعلامات `CREATE` و`DROP` و`ALTER` و`RENAME` إلا في الخادم الحالي الذي تُنفَّذ عليه. وفي إعداد عنقودي، يمكن تشغيل مثل هذه الاستعلامات بصورة موزعة باستخدام عبارة `ON CLUSTER`.

على سبيل المثال، ينشئ الاستعلام التالي جدول `all_hits` من نوع `Distributed` على كل مضيف في `cluster`:

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

لكي تُنفَّذ هذه الاستعلامات بشكل صحيح، يجب أن يكون لدى كل مضيف تعريف العنقود نفسه (ولتسهيل مزامنة الإعدادات، يمكنك استخدام الاستبدالات من ZooKeeper). كما يجب أن تتصل بخوادم ZooKeeper.

سيُنفَّذ الإصدار المحلي من الاستعلام في نهاية المطاف على كل مضيف في العنقود، حتى إذا كان بعض المضيفين غير متاحين حاليًا.

:::important
ترتيب تنفيذ الاستعلامات على مستوى المضيف الواحد مضمون.
:::