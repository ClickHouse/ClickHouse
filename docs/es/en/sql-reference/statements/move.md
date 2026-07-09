---
description: 'Documentación de la sentencia MOVE para entidades de acceso'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'Sentencia MOVE para entidades de acceso'
doc_type: 'reference'
---

Esta sentencia permite mover una entidad de acceso de un almacenamiento de acceso a otro.

Sintaxis:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

Actualmente, hay cinco tipos de almacenamiento de acceso en ClickHouse:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

Ejemplos:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```