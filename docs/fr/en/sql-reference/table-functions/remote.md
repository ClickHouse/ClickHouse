---
description: 'La fonction de table `remote` permet d’accéder à des serveurs distants à la volée,
  c’est-à-dire sans créer de table [Distributed](../../engines/table-engines/special/distributed.md). La fonction de table `remoteSecure` fonctionne comme
  `remote`, mais via une connexion sécurisée.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

La fonction de table `remote` permet d’accéder à des serveurs distants à la volée, c’est-à-dire sans créer de table [Distributed](../../engines/table-engines/special/distributed.md). La fonction de table `remoteSecure` fonctionne comme `remote`, mais via une connexion sécurisée.

Les deux fonctions peuvent être utilisées dans des requêtes `SELECT` et `INSERT`.

<div id="syntax">
  ## Syntaxe
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## Paramètres
</div>

| Argument         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | Adresse d’un serveur distant ou expression générant plusieurs adresses de serveurs distants. Format : `host` ou `host:port`.<br /><br />    `host` peut être indiqué sous forme de nom de serveur, ou d’adresse IPv4 ou IPv6. Une adresse IPv6 doit être indiquée entre `[]`.<br /><br />    `port` est le port TCP du serveur distant. Si le port est omis, la fonction de table `remote` utilise [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) du fichier de configuration du serveur (9000 par défaut), et la fonction de table `remoteSecure` utilise [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) (9440 par défaut).<br /><br />    Pour les adresses IPv6, un port est obligatoire.<br /><br />    Si seul le paramètre `addresses_expr` est spécifié, `db` et `table` utiliseront `system.one` par défaut.<br /><br />    Type : [String](../../sql-reference/data-types/string.md). |
| `db`             | Nom de la base de données. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `table`          | Nom de la table. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `user`           | Nom d’utilisateur. S’il n’est pas spécifié, `default` est utilisé. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `password`       | Mot de passe de l’utilisateur. S’il n’est pas spécifié, un mot de passe vide est utilisé. Type : [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `sharding_key`   | Clé de partitionnement permettant de répartir les données entre les nœuds. Par exemple : `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Type : [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

Les arguments peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md).

<div id="returned-value">
  ## Valeur renvoyée
</div>

Une table située sur un serveur distant.

<div id="usage">
  ## Utilisation
</div>

Comme les fonctions de table `remote` et `remoteSecure` rétablissent la connexion pour chaque requête, il est recommandé d’utiliser à la place une table `Distributed`. De plus, si des noms d’hôte sont définis, les noms sont résolus et les erreurs ne sont pas comptabilisées lors de l’utilisation de différentes répliques. Lors du traitement d’un grand nombre de requêtes, créez toujours la table `Distributed` à l’avance et n’utilisez pas la fonction de table `remote`.

La fonction de table `remote` peut être utile dans les cas suivants :

* Migration ponctuelle de données d’un système à un autre
* Accès à un serveur spécifique pour comparer les données, effectuer du débogage et réaliser des tests, c.-à-d. des connexions ad hoc.
* Requêtes entre différents clusters ClickHouse à des fins de recherche.
* Requêtes distribuées peu fréquentes effectuées manuellement.
* Requêtes distribuées pour lesquelles l’ensemble des serveurs est redéfini à chaque fois.

<div id="addresses">
  ### Adresses
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Plusieurs adresses peuvent être séparées par des virgules. Dans ce cas, ClickHouse utilisera le traitement distribué et enverra la requête à toutes les adresses spécifiées (comme des shards contenant des données différentes). Exemple :

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## Exemples
</div>

<div id="selecting-data-from-a-remote-server">
  ### Sélection de données à partir d’un serveur distant :
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

Ou à l’aide de [collections nommées](/fr/operations/named-collections.md) :

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### Insertion de données dans une table d’un serveur distant :
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### Migration de tables d’un système à un autre :
</div>

Cet exemple utilise une table issue d’un jeu de données d’exemple. La base de données est `imdb` et la table est `actors`.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### Sur le système ClickHouse source (le système qui héberge actuellement les données)
</div>

* Vérifiez la base de données source et le nom de la table (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* Récupérez l’instruction CREATE TABLE à partir de la source :

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

Réponse

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### Sur le système ClickHouse de destination
</div>

* Créez la base de données de destination :

  ```sql
  CREATE DATABASE imdb
  ```

* À l’aide de l’instruction CREATE TABLE du système source, créez la table de destination :

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### De retour sur le déploiement source
</div>

Insérez des données dans la nouvelle base de données et la nouvelle table créées sur le système distant. Vous aurez besoin de l’hôte, du port, du nom d’utilisateur, du mot de passe, de la base de données de destination et de la table de destination.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## Globbing
</div>

Les motifs dans `{ }` sont utilisés pour générer un ensemble de shards et pour spécifier des répliques. S&#39;il y a plusieurs paires de `{ }`, le produit cartésien des ensembles correspondants est généré.

Les types de motifs suivants sont pris en charge.

* `{a,b,c}` - Représente l&#39;une des chaînes alternatives `a`, `b` ou `c`. Le motif est remplacé par `a` dans la première adresse de shard, puis par `b` dans la deuxième, et ainsi de suite. Par exemple, `example0{1,2}-1` génère les adresses `example01-1` et `example02-1`.
* `{N..M}` - Une plage de nombres. Ce motif génère des adresses de shard avec des indices croissants de `N` à `M` inclus. Par exemple, `example0{1..2}-1` génère `example01-1` et `example02-1`.
* `{0n..0m}` - Une plage de nombres avec des zéros non significatifs. Ce motif conserve les zéros en tête dans les indices. Par exemple, `example{01..03}-1` génère `example01-1`, `example02-1` et `example03-1`.
* `{a|b}` - Un nombre quelconque de variantes séparées par `|`. Ce motif permet de spécifier les répliques. Par exemple, `example01-{1|2}` génère les répliques `example01-1` et `example01-2`.

La requête sera envoyée à la première réplique saine. Cependant, pour `remote`, les répliques sont parcourues dans l&#39;ordre actuellement défini par le paramètre [load&#95;balancing](../../operations/settings/settings.md#load_balancing).
Le nombre d&#39;adresses générées est limité par le paramètre [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses).