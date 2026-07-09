---
description: 'Documentation sur le rôle'
sidebar_label: 'ROLE'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

Crée de nouveaux [rôles](../../../guides/sre/user-management/index.md#role-management). Un rôle est un ensemble de [privilèges](/fr/sql-reference/statements/grant#granting-privilege-syntax). Un [utilisateur](../../../sql-reference/statements/create/user.md) auquel un rôle est attribué obtient tous les privilèges associés à ce rôle.

Syntaxe :

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## Gestion des rôles
</div>

Un utilisateur peut se voir attribuer plusieurs rôles. Les utilisateurs peuvent activer les rôles qui leur sont attribués dans n’importe quelle combinaison à l’aide de l’instruction [SET ROLE](../../../sql-reference/statements/set-role.md). Le périmètre effectif des privilèges correspond à l’ensemble cumulé de tous les privilèges de tous les rôles activés. Si des privilèges sont accordés directement à son compte utilisateur, ils sont également cumulés avec les privilèges accordés par les rôles.

Un utilisateur peut avoir des rôles par défaut, qui s’appliquent lors de la connexion. Pour définir les rôles par défaut, utilisez l’instruction [SET DEFAULT ROLE](/fr/sql-reference/statements/set-role#set-default-role) ou l’instruction [ALTER USER](/fr/sql-reference/statements/alter/user).

Pour révoquer un rôle, utilisez l’instruction [REVOKE](../../../sql-reference/statements/revoke.md).

Pour supprimer un rôle, utilisez l’instruction [DROP ROLE](/fr/sql-reference/statements/drop#drop-role). Le rôle supprimé est automatiquement révoqué pour tous les utilisateurs et rôles auxquels il était attribué.

<div id="examples">
  ## Exemples
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Cette suite de requêtes crée le rôle `accountant`, qui dispose du privilège de lire les données de la base de données `db`.

Attribution du rôle à l’utilisateur `mira` :

```sql
GRANT accountant TO mira;
```

Une fois le rôle attribué, l&#39;utilisateur peut l&#39;activer et exécuter les requêtes autorisées. Par exemple :

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```