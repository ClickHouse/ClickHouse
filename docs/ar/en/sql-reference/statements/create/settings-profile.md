---
description: 'توثيق SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 43
slug: /sql-reference/statements/create/settings-profile
title: 'CREATE SETTINGS PROFILE'
doc_type: 'مرجع'
---

يُنشئ [ملفات تعريف الإعدادات](../../../guides/sre/user-management/index.md#settings-profiles-management) التي يمكن تعيينها لمستخدم أو دور.

الصيغة:

```sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

تتيح العبارة `ON CLUSTER` إنشاء ملفات تعريف الإعدادات على مستوى العنقود، راجع [DDL الموزعة](../../../sql-reference/distributed-ddl.md).

<div id="example">
  ## مثال
</div>

أنشئ مستخدمًا:

```sql
CREATE USER robin IDENTIFIED BY 'password';
```

أنشئ ملف تعريف الإعدادات `max_memory_usage_profile` مع قيمة وقيود لإعداد `max_memory_usage`، ثم أسنده إلى المستخدم `robin`:

```sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```