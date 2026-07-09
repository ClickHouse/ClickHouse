---
description: 'Documentation relative à la gestion du TTL de table'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'Gestion du TTL de table'
doc_type: 'reference'
---

:::note
Si vous recherchez plus d’informations sur l’utilisation de TTL pour gérer les données anciennes, consultez le guide utilisateur [Gérer les données avec TTL](/fr/guides/developer/ttl.md). La documentation ci-dessous explique comment modifier ou supprimer une règle TTL existante.
:::

<div id="modify-ttl">
  ## MODIFY TTL
</div>

Vous pouvez modifier le [TTL de table](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) à l’aide d’une requête de la forme suivante :

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## REMOVE TTL
</div>

La propriété TTL peut être supprimée d’une table à l’aide de la requête suivante :

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Exemple**

Considérez la table suivante avec un `TTL de table` défini au niveau de la table :

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Exécutez `OPTIMIZE` pour forcer le nettoyage TTL :

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

La deuxième ligne a été supprimée de la table.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Supprimez maintenant le `TTL de table` avec la requête suivante :

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Réinsérez la ligne supprimée et relancez le nettoyage `TTL` avec `OPTIMIZE` :

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

Le `TTL` n’est plus défini, donc la deuxième ligne n’est pas supprimée :

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**Voir aussi**

* En savoir plus sur l’[expression TTL](../../../sql-reference/statements/create/table.md#ttl-expression).
* Modifier une colonne [avec TTL](/fr/sql-reference/statements/alter/ttl).