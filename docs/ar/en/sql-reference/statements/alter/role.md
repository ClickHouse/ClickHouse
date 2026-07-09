---
description: 'مرجع ROLE'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: 'reference'
---

يعدّل الأدوار.

البنية:

```sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

يُعدّ `SET variable = value` اسمًا بديلًا لـ `MODIFY SETTING variable = value`: إذ يغيّر إعدادًا واحدًا فقط مع الإبقاء على بقية الإعدادات، بخلاف عبارة `SETTINGS` بصورتها المجرّدة، التي تستبدل قائمة الإعدادات بالكامل وتزيل أيضًا جميع ملفات التعريف الموروثة (الأصلية).