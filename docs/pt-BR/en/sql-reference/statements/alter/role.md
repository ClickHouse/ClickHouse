---
description: 'Documentação sobre Role'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: 'reference'
---

Altera roles.

Sintaxe:

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

`SET variable = value` é um alias de `MODIFY SETTING variable = value`: ele altera uma única configuração sem afetar as demais, ao contrário da cláusula `SETTINGS` sem modificador, que substitui toda a lista de configurações e também remove todos os perfis herdados (perfil pai).