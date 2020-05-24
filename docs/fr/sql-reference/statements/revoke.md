---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: REVOKE
---

# REVOKE {#revoke}

Révoque les privilèges des utilisateurs ou rôles.

## Syntaxe {#revoke-syntax}

**Révocation des privilèges des utilisateurs**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Révocation des rôles des utilisateurs**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Description {#revoke-description}

Pour révoquer certains privilèges, vous pouvez utiliser un privilège de portée plus large que vous envisagez de révoquer. Par exemple, si un utilisateur a la `SELECT (x,y)` privilège, administrateur peut effectuer `REVOKE SELECT(x,y) ...`, ou `REVOKE SELECT * ...` ou même `REVOKE ALL PRIVILEGES ...` requête de révoquer ce privilège.

### Révocations Partielles {#partial-revokes-dscr}

Vous pouvez révoquer une partie d'un privilège. Par exemple, si un utilisateur a la `SELECT *.*` Privilège vous pouvez révoquer un privilège pour lire les données d'une table ou d'une base de données.

## Exemple {#revoke-example}

Subvention de l' `john` compte utilisateur avec le privilège de sélectionner parmi toutes les bases de données `accounts` un:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Subvention de l' `mira` compte utilisateur avec le privilège de sélectionner parmi toutes les colonnes `accounts.staff` tableau à l'exception de la `wage` un.

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

{## [Article Original](https://clickhouse.tech/docs/en/operations/settings/settings/) ##}
