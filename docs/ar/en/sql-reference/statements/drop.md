---
description: 'توثيق عبارات DROP'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'عبارات DROP'
doc_type: 'reference'
---

تحذف كيانًا موجودًا. إذا جرى تحديد العبارة `IF EXISTS`، فلن تُرجع هذه الاستعلامات خطأً إذا لم يكن الكيان موجودًا. وإذا جرى تحديد المُعدِّل `SYNC`، فسيُحذف الكيان من دون تأخير.

<div id="drop-database">
  ## DROP DATABASE
</div>

يحذف جميع الجداول داخل قاعدة البيانات `db`، ثم يحذف قاعدة البيانات `db` نفسها.

البنية:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

يحذف جدولًا واحدًا أو أكثر.

:::tip
للتراجع عن حذف جدول، يُرجى الاطلاع على [UNDROP TABLE](/ar/sql-reference/statements/undrop.md)
:::

الصيغة:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

القيود:

* إذا تم تحديد البند `IF EMPTY`، فلن يتحقق الخادم من خلو الجدول إلا على النسخة المتماثلة التي استقبلت الاستعلام.
* حذف عدة جداول دفعة واحدة ليس عملية ذرية، أي إذا فشل حذف أحد الجداول، فلن تُحذف الجداول اللاحقة.

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

يحذف هذا الأمر القاموس.

البنية:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

يحذف مستخدمًا.

البنية:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

يحذف دورًا. ويُلغى الدور المحذوف من جميع الكيانات التي كان مُسنَدًا إليها.

الصياغة:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

يحذف ROW POLICY. وتُلغى ROW POLICY المحذوفة من جميع الكيانات التي كانت مُسندة إليها.

الصياغة:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

يحذف سياسة إخفاء.

البنية:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

يحذف QUOTA. وتُلغى قيمة QUOTA المحذوفة من جميع الكيانات التي كانت مُعيّنة لها.

الصيغة:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

يحذف ملف تعريف إعدادات. ويُلغى ملف تعريف الإعدادات المحذوف من جميع الكيانات التي كان مُعيَّنًا لها.

الصياغة:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

يحذف طريقة عرض. ويمكن أيضًا حذف طرق العرض بأمر `DROP TABLE`، لكن `DROP VIEW` يتحقق من أن `[db.]name` هو طريقة عرض.

الصيغة:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

يحذف دالة معرّفة من قبل المستخدم تم إنشاؤها بواسطة [CREATE FUNCTION](./create/function.md).
لا يمكن حذف دوال النظام.

**الصيغة**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**مثال**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

يحذف مجموعة مسمّاة.

**الصيغة**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**مثال**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```