---
description: 'Документация по оператору MOVE для объекта управления доступом'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'Оператор MOVE для объекта управления доступом'
doc_type: 'reference'
---

Этот оператор позволяет перемещать объект управления доступом из одного хранилища доступа в другое.

Синтаксис:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

В настоящее время в ClickHouse есть пять хранилищ для управления доступом:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

Примеры:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```