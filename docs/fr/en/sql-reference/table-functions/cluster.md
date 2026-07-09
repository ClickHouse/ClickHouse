---
description: 'Permet d’accéder à tous les shards (configurés dans la section `remote_servers`)
  d’un cluster sans créer de table Distributed.'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'reference'
---

Permet d’accéder à tous les shards (configurés dans la section `remote_servers`) d’un cluster sans créer de table [Distributed](../../engines/table-engines/special/distributed.md). Une seule réplique de chaque shard est interrogée.

Fonction `clusterAllReplicas` — identique à `cluster`, mais toutes les répliques sont interrogées. Chaque réplique d’un cluster est utilisée comme shard/connexion distinct(e).

:::note
Tous les clusters disponibles sont listés dans la table [system.clusters](../../operations/system-tables/clusters.md).
:::

<div id="syntax">
  ## Syntaxe
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## Arguments
</div>

| Arguments                   | Type                                                                                                                                                            |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`              | Nom d’un cluster utilisé pour constituer un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux ; `default` si non spécifié. |
| `db.table` or `db`, `table` | Nom d’une base de données et d’une table.                                                                                                                       |
| `sharding_key`              | Une clé de partitionnement. Facultatif. Doit être spécifiée si le cluster comporte plus d’un shard.                                                             |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Le jeu de données des clusters.

<div id="using_macros">
  ## Utilisation des macros
</div>

`cluster_name` peut contenir des macros, c’est-à-dire des substitutions entre `{}`. La valeur substituée est extraite de la section [macros](../../operations/server-configuration-parameters/settings.md#macros) du fichier de configuration du serveur.

Exemple :

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## Utilisation et recommandations
</div>

L’utilisation des fonctions de table `cluster` et `clusterAllReplicas` est moins efficace que la création d’une table `Distributed`, car dans ce cas, la connexion au serveur est réétablie pour chaque requête. Lors du traitement d’un grand nombre de requêtes, créez toujours la table `Distributed` à l’avance et n’utilisez pas les fonctions de table `cluster` et `clusterAllReplicas`.

Les fonctions de table `cluster` et `clusterAllReplicas` peuvent être utiles dans les cas suivants :

* Accéder à un cluster spécifique pour comparer des données, effectuer du débogage et réaliser des tests.
* Exécuter des requêtes sur différents clusters et répliques ClickHouse à des fins de recherche.
* Requêtes distribuées occasionnelles exécutées manuellement.

Les paramètres de connexion tels que `host`, `port`, `user`, `password`, `compression` et `secure` sont repris de la section de configuration `<remote_servers>`. Voir les détails dans [moteur Distributed](../../engines/table-engines/special/distributed.md).

<div id="related">
  ## Voir aussi
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)