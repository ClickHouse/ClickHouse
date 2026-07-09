---
description: 'توثيق تعليمة REVOKE'
sidebar_label: 'REVOKE'
sidebar_position: 39
slug: /sql-reference/statements/revoke
title: 'تعليمة REVOKE'
doc_type: 'reference'
---

يلغي الامتيازات من المستخدمين أو الأدوار.

<div id="syntax">
  ## الصيغة
</div>

**إلغاء امتيازات المستخدمين**

```sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**سحب الأدوار من المستخدمين**

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

<div id="description">
  ## الوصف
</div>

لإلغاء بعض الامتيازات، يمكنك استخدام امتياز ذي نطاق أوسع من الامتياز الذي تنوي إلغاءه. على سبيل المثال، إذا كان لدى مستخدم امتياز `SELECT (x,y)`، فيمكن للمسؤول تنفيذ الاستعلام `REVOKE SELECT(x,y) ...`، أو `REVOKE SELECT * ...`، أو حتى `REVOKE ALL PRIVILEGES ...` لإلغاء هذا الامتياز.

<div id="partial-revokes">
  ### الإلغاء الجزئي للامتيازات
</div>

يمكنك إلغاء جزء من امتياز ما. على سبيل المثال، إذا كان لدى مستخدم الامتياز `SELECT *.*`، فيمكنك إلغاء امتياز قراءة البيانات من جدول معيّن أو من قاعدة بيانات معيّنة.

<div id="examples">
  ## أمثلة
</div>

امنح حساب المستخدم `john` امتياز `SELECT` على جميع قواعد البيانات، باستثناء `accounts`:

```sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

امنح حساب المستخدم `mira` امتياز `SELECT` على جميع أعمدة الجدول `accounts.staff`، باستثناء العمود `wage`.

```sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[المقال الأصلي](/ar/operations/settings/settings/)