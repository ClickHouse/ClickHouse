---
description: 'Documentation des instructions TRUNCATE'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'Instructions TRUNCATE'
doc_type: 'reference'
---

Dans ClickHouse, l’instruction `TRUNCATE` permet de supprimer rapidement toutes les données d’une table ou d’une base de données sans en modifier la structure.

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| Paramètre            | Description                                                                                                                                                                            |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IF EXISTS`          | Évite une erreur si la table n&#39;existe pas. Si cet élément est omis, la requête renvoie une erreur.                                                                                 |
| `db.name`            | Nom de la base de données facultatif.                                                                                                                                                  |
| `ON CLUSTER cluster` | Exécute la commande sur l&#39;ensemble d&#39;un cluster spécifié.                                                                                                                      |
| `SYNC`               | Rend la troncature synchrone sur les répliques lors de l&#39;utilisation de tables répliquées. Si cet élément est omis, la troncature s&#39;effectue de manière asynchrone par défaut. |

Vous pouvez utiliser le paramètre [alter&#95;sync](/fr/operations/settings/settings#alter_sync) pour configurer l&#39;attente de l&#39;exécution des actions sur les répliques.

Vous pouvez spécifier combien de temps (en secondes) attendre que des répliques inactives exécutent des requêtes `TRUNCATE` avec le paramètre [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/fr/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Si `alter_sync` est défini sur `2` et que certaines répliques restent inactives plus longtemps que la durée spécifiée par le paramètre `replication_wait_for_inactive_replica_timeout`, une exception `UNFINISHED` est levée.
:::

La requête `TRUNCATE TABLE` n&#39;est **pas prise en charge** pour les moteurs de table suivants :

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## TRUNCATE de toutes les tables
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| Paramètre                               | Description                                                      |
| --------------------------------------- | ---------------------------------------------------------------- |
| `ALL`                                   | Supprime les données de toutes les tables de la base de données. |
| `IF EXISTS`                             | Empêche une erreur si la base de données n&#39;existe pas.       |
| `db`                                    | Le nom de la base de données.                                    |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Filtre les tables selon un motif.                                |
| `ON CLUSTER cluster`                    | Exécute la commande sur l&#39;ensemble du cluster.               |

Supprime toutes les données de toutes les tables d&#39;une base de données.

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| Paramètre            | Description                                                       |
| -------------------- | ----------------------------------------------------------------- |
| `IF EXISTS`          | Empêche une erreur si la base de données n&#39;existe pas.        |
| `db`                 | Le nom de la base de données.                                     |
| `ON CLUSTER cluster` | Exécute la commande sur l&#39;ensemble d&#39;un cluster spécifié. |

Supprime toutes les tables d&#39;une base de données, mais conserve la base de données elle-même. Lorsque la clause `IF EXISTS` est omise, la requête renvoie une erreur si la base de données n&#39;existe pas.

:::note
`TRUNCATE DATABASE` n&#39;est pas pris en charge pour les bases de données `Replicated`. Utilisez simplement `DROP` puis `CREATE` pour la base de données.
:::