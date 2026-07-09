---
description: 'توثيق للتعامل مع القيود'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'التعامل مع القيود'
doc_type: 'reference'
---

يمكن إضافة القيود أو تعديلها أو حذفها باستخدام الصيغة التالية:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

كما هو الحال عند إنشاء جدول، يمكن تعريف القيد إما على أنه `CHECK` (يُفرَض عند `INSERT`) أو `ASSUME` (يثق به المُحسِّن من دون التحقق منه). راجع [القيود](../../../sql-reference/statements/create/table.md#constraints) لمعرفة الفرق بينهما.

تستبدل `MODIFY CONSTRAINT` تعريف قيد موجود مع الإبقاء على موضعه في تعريف الجدول. ويمكنها أيضًا تغيير نوع القيد (على سبيل المثال، من `CHECK` إلى `ASSUME`). وهي تكافئ حذف القيد ثم إضافته مرة أخرى بالتعريف الجديد. وإذا لم يكن القيد موجودًا، فسيُطلق الاستعلام خطأ ما لم يتم تحديد `IF EXISTS`.

اطّلع على المزيد حول [القيود](../../../sql-reference/statements/create/table.md#constraints).

ستضيف الاستعلامات بيانات وصفية عن القيود إلى الجدول أو تغيّرها أو تزيلها منه، لذا تُعالَج فورًا.

:::tip
**لن يُنفَّذ** فحص القيد على البيانات الموجودة إذا تمت إضافته أو تعديله.
:::

تُبث جميع التغييرات على الجداول المكررة إلى ZooKeeper، وستُطبَّق على النسخ المتماثلة الأخرى أيضًا.