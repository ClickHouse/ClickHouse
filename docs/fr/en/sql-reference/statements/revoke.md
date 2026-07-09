---
description: 'Documentation de l’instruction REVOKE'
sidebar_label: 'REVOKE'
sidebar_position: 39
slug: /sql-reference/statements/revoke
title: 'Instruction REVOKE'
doc_type: 'reference'
---

Révoque des privilèges accordés à des utilisateurs ou à des rôles.

<div id="syntax">
  ## Syntaxe
</div>

**Révocation des privilèges accordés aux utilisateurs**

```sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Révocation de rôles attribués à des utilisateurs**

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

<div id="description">
  ## Description
</div>

Pour révoquer un privilège, vous pouvez utiliser un privilège d’une portée plus large que celui que vous souhaitez révoquer. Par exemple, si un utilisateur dispose du privilège `SELECT (x,y)`, un administrateur peut exécuter la requête `REVOKE SELECT(x,y) ...`, `REVOKE SELECT * ...` ou même `REVOKE ALL PRIVILEGES ...` pour révoquer ce privilège.

<div id="partial-revokes">
  ### Révocations partielles
</div>

Vous pouvez révoquer une partie d’un privilège. Par exemple, si un utilisateur dispose du privilège `SELECT *.*`, vous pouvez lui révoquer le privilège de lire les données d’une table ou d’une base de données spécifique.

<div id="examples">
  ## Exemples
</div>

Accordez au compte utilisateur `john` le privilège de sélectionner sur toutes les bases de données, à l’exception de `accounts` :

```sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Accordez au compte utilisateur `mira` le privilège de sélectionner toutes les colonnes de la table `accounts.staff`, à l’exception de `wage`.

```sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[Article original](/fr/operations/settings/settings/)