---
description: 'Documentação do SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 43
slug: /sql-reference/statements/create/settings-profile
title: 'CREATE SETTINGS PROFILE'
doc_type: 'reference'
---

Cria [perfis de configurações](../../../guides/sre/user-management/index.md#settings-profiles-management) que podem ser atribuídos a um usuário ou a uma role.

Sintaxe:

```sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

A cláusula `ON CLUSTER` permite criar perfis de configuração em um cluster; consulte [DDL distribuído](../../../sql-reference/distributed-ddl.md).

<div id="example">
  ## Exemplo
</div>

Crie um usuário:

```sql
CREATE USER robin IDENTIFIED BY 'password';
```

Crie o perfil de configurações `max_memory_usage_profile`, com valor e restrições para a configuração `max_memory_usage`, e atribua-o ao usuário `robin`:

```sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```