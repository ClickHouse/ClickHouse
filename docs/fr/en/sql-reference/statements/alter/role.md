---
description: 'Documentation sur le rôle'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: 'reference'
---

Modifie les rôles.

Syntaxe :

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

`SET variable = value` est un alias de `MODIFY SETTING variable = value` : il modifie un seul paramètre sans toucher aux autres, contrairement à la clause `SETTINGS` seule, qui remplace toute la liste des paramètres et supprime également tous les profils hérités (parents).