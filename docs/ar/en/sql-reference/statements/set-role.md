---
description: 'توثيق لعبارة SET ROLE'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'عبارة SET ROLE'
doc_type: 'reference'
---

يُفعِّل الأدوار الخاصة بالمستخدم الحالي.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

يُعيّن الأدوار الافتراضية لمستخدمٍ ما.

تُفعَّل الأدوار الافتراضية تلقائيًا عند تسجيل دخول المستخدم. ولا يمكنك تعيين أدوار افتراضية إلا من بين الأدوار الممنوحة مسبقًا. وإذا لم يكن الدور ممنوحًا للمستخدم، يطرح ClickHouse استثناء.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## أمثلة
</div>

عيّن لمستخدم عدة أدوار افتراضية:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

عيّن جميع الأدوار الممنوحة كأدوار افتراضية للمستخدم:

```sql
SET DEFAULT ROLE ALL TO user
```

احذف الأدوار الافتراضية من مستخدم:

```sql
SET DEFAULT ROLE NONE TO user
```

عيّن جميع الأدوار الممنوحة كأدوار افتراضية، باستثناء الدورين المحددين `role1` و`role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```