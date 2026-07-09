---
description: 'Documentação da instrução MOVE para entidade de acesso'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'Instrução MOVE para entidade de acesso'
doc_type: 'referência'
---

Esta instrução permite mover uma entidade de acesso de um armazenamento de acesso para outro.

Sintaxe:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

Atualmente, há cinco mecanismos de armazenamento de acesso no ClickHouse:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

Exemplos:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```