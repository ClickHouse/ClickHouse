---
description: "Documentation de l'instruction MOVE sur une entité d'accès"
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: "Instruction MOVE sur une entité d'accès"
doc_type: 'référence'
---

Cette instruction permet de déplacer une entité d&#39;accès d&#39;un stockage des accès à un autre.

Syntaxe :

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

Actuellement, il existe cinq modes de stockage des accès dans ClickHouse :

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

Exemples :

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```