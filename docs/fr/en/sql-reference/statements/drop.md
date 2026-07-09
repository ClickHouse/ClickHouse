---
description: 'Documentation des instructions DROP'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'Instructions DROP'
doc_type: 'reference'
---

Supprime une entité existante. Si la clause `IF EXISTS` est spécifiée, ces instructions ne renvoient pas d’erreur si l’entité n’existe pas. Si le modificateur `SYNC` est spécifié, l’entité est supprimée immédiatement.

<div id="drop-database">
  ## DROP DATABASE
</div>

Supprime toutes les tables de la base de données `db`, puis la base de données `db` elle-même.

Syntaxe :

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

Supprime une ou plusieurs tables.

:::tip
Pour annuler la suppression d’une table, consultez [UNDROP TABLE](/fr/sql-reference/statements/undrop.md)
:::

Syntaxe :

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Limitations :

* Si la clause `IF EMPTY` est spécifiée, le serveur vérifie si la table est vide uniquement sur la réplique qui a reçu la requête.
* La suppression de plusieurs tables à la fois n&#39;est pas une opération atomique, c&#39;est-à-dire que si la suppression d&#39;une table échoue, les tables suivantes ne seront pas supprimées.

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

Supprime le dictionnaire.

Syntaxe :

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

Supprime un utilisateur.

Syntaxe:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

Supprime un rôle. Le rôle supprimé est révoqué pour toutes les entités auxquelles il était attribué.

Syntaxe :

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

Supprime une row politique. La row politique supprimée est révoquée pour toutes les entités auxquelles elle était attribuée.

Syntaxe :

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

Supprime une politique de masquage.

Syntaxe :

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

Supprime un quota. Le quota supprimé est révoqué pour toutes les entités auxquelles il avait été attribué.

Syntaxe :

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

Supprime un profil de paramètres. Le profil de paramètres supprimé est révoqué pour toutes les entités auxquelles il était attribué.

Syntaxe :

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

Supprime une vue. Les vues peuvent aussi être supprimées à l’aide d’une commande `DROP TABLE`, mais `DROP VIEW` vérifie que `[db.]name` est bien une vue.

Syntaxe :

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

Supprime une user defined function créée avec [CREATE FUNCTION](./create/function.md).
Les fonctions système ne peuvent pas être supprimées.

**Syntaxe**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Exemple**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

Supprime une collection nommée.

**Syntaxe**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Exemple**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```