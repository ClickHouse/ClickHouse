---
description: 'توثيق تعليمة MOVE الخاصة بكيان الوصول'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'تعليمة MOVE الخاصة بكيان الوصول'
doc_type: 'reference'
---

تتيح هذه التعليمة نقل كيان وصول من مخزن وصول إلى مخزن وصول آخر.

الصيغة:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

يوجد حاليًا خمسة أنواع من مخازن الوصول في ClickHouse:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

أمثلة:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```