---
description: 'Le moteur est basé sur le moteur Atomic. Il prend en charge la réplication des
  métadonnées grâce à l’écriture du journal DDL dans ZooKeeper, puis à son exécution sur toutes les répliques
  d’une base de données donnée.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

Le moteur est basé sur le moteur [Atomic](../../engines/database-engines/atomic.md). Il prend en charge la réplication des métadonnées grâce à l’écriture du journal DDL dans ZooKeeper, puis à son exécution sur toutes les répliques d’une base de données donnée.

Un serveur ClickHouse peut héberger plusieurs bases de données répliquées, actives et mises à jour simultanément. En revanche, il ne peut pas y avoir plusieurs répliques d’une même base de données répliquée.

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Paramètres du moteur**

* `zoo_path` — chemin ZooKeeper. Un même chemin ZooKeeper correspond à une même base de données.
* `shard_name` — nom du shard. Les répliques de la base de données sont regroupées en shards selon `shard_name`.
* `replica_name` — nom de la réplique. Les noms de réplique doivent être différents pour toutes les répliques d&#39;un même shard.

Les paramètres peuvent être omis ; dans ce cas, les paramètres manquants sont remplacés par leurs valeurs par défaut.

Si `zoo_path` contient la macro `{uuid}`, il faut spécifier un UUID explicite ou ajouter [ON CLUSTER](../../sql-reference/distributed-ddl.md) à l&#39;instruction CREATE afin de garantir que toutes les répliques utilisent le même UUID pour cette base de données.

Pour les tables [ReplicatedMergeTree](/fr/engines/table-engines/mergetree-family/replication), si aucun argument n&#39;est fourni, les arguments par défaut suivants sont utilisés : `/clickhouse/tables/{uuid}/{shard}` et `{replica}`. Ils peuvent être modifiés dans les paramètres du serveur [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) et [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name). La macro `{uuid}` est remplacée par l&#39;UUID de la table ; `{shard}` et `{replica}` sont remplacées par des valeurs issues de la configuration du serveur, et non des arguments du moteur de base de données. À l&#39;avenir, il sera toutefois possible d&#39;utiliser `shard_name` et `replica_name` de la base de données Replicated.

Un cluster ZooKeeper auxiliaire est également pris en charge pour stocker les métadonnées d&#39;une base de données répliquée au lieu d&#39;utiliser le cluster ZooKeeper par défaut. Vous pouvez utiliser SQL pour créer la base de données répliquée avec un cluster ZooKeeper auxiliaire comme suit :

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## Spécificités et recommandations
</div>

Les requêtes DDL avec une base de données `Replicated` fonctionnent de manière similaire aux requêtes [ON CLUSTER](../../sql-reference/distributed-ddl.md), avec toutefois quelques différences mineures.

Tout d&#39;abord, la requête DDL tente de s&#39;exécuter sur l&#39;initiateur (l&#39;hôte qui a initialement reçu la requête de l&#39;utilisateur). Si la requête n&#39;aboutit pas, l&#39;utilisateur reçoit immédiatement une erreur et les autres hôtes ne tentent pas de l&#39;exécuter. Si la requête a bien été exécutée sur l&#39;initiateur, tous les autres hôtes la réessaieront automatiquement jusqu&#39;à ce qu&#39;elle aboutisse. L&#39;initiateur tentera d&#39;attendre que la requête s&#39;exécute sur les autres hôtes (pas plus longtemps que [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) et renverra une table contenant les statuts d&#39;exécution de la requête sur chaque hôte.

Le comportement en cas d&#39;erreur est régi par le paramètre [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode) ; pour une base de données `Replicated`, il est préférable de le définir sur `null_status_on_timeout` — c&#39;est-à-dire que si certains hôtes n&#39;ont pas eu le temps d&#39;exécuter la requête dans le délai [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), il ne faut pas lever d&#39;exception, mais afficher pour eux le statut `NULL` dans la table.

La table système [system.clusters](../../operations/system-tables/clusters.md) contient un cluster portant le même nom que la base de données répliquée, composé de toutes les répliques de la base de données. Ce cluster est mis à jour automatiquement lors de la création ou de la suppression de répliques, et peut être utilisé pour les tables [Distributed](/fr/engines/table-engines/special/distributed).

Lors de la création d&#39;une nouvelle réplique de la base de données, cette réplique crée elle-même les tables. Si la réplique a été indisponible pendant une longue période et a pris du retard dans le journal de réplication, elle compare ses métadonnées locales aux métadonnées actuelles dans ZooKeeper, déplace les tables supplémentaires avec leurs données vers une base de données distincte non répliquée (afin de ne rien supprimer accidentellement), crée les tables manquantes et met à jour les noms des tables si elles ont été renommées. Les données sont répliquées au niveau de `ReplicatedMergeTree` ; autrement dit, si la table n&#39;est pas répliquée, les données ne le seront pas non plus (la base de données n&#39;est responsable que des métadonnées).

Les requêtes [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) sont autorisées, mais ne sont pas répliquées. Le moteur de base de données ajoute/récupère/supprime uniquement la partition ou la partie sur la réplique courante. Toutefois, si la table elle-même utilise un moteur de table Replicated, les données seront répliquées après l&#39;utilisation de `ATTACH`.

Si vous avez seulement besoin de configurer un cluster sans maintenir la réplication des tables, consultez la fonctionnalité [Cluster Discovery](../../operations/cluster-discovery.md).

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Création d’un cluster avec trois hôtes :

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Création d’une base de données sur un cluster avec des paramètres implicites :

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

Exécution de la requête DDL :

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Affichage de la table système :

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Création d’une table distribuée et insertion des données :

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Ajout d’une réplique sur un hôte supplémentaire :

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Ajout d’une réplique sur un autre hôte si la macro `{uuid}` est utilisée dans `zoo_path` :

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

La configuration du cluster se présentera ainsi :

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

La table distribuée récupérera également des données du nouvel hôte :

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## Paramètres
</div>

Les paramètres suivants sont pris en charge :

| Paramètre                                                                    | Valeur par défaut              | Description                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | Ne pas récupérer automatiquement la réplique si le ratio entre les tables obsolètes et l’ensemble des tables est supérieur                                                                                                                                                                                                                                                                   |
| `max_replication_lag_to_enqueue`                                             | 50                             | La réplique lèvera une exception lors d’une tentative d’exécution d’une requête si son retard de réplication est supérieur                                                                                                                                                                                                                                                                   |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | Les répliques essaieront d’annuler la requête si le délai d’attente est dépassé, mais que l’hôte initiateur ne l’a pas encore exécutée                                                                                                                                                                                                                                                       |
| `collection_name`                                                            |                                | Nom d’une collection définie dans la configuration du serveur, où sont définies toutes les informations d’authentification du cluster                                                                                                                                                                                                                                                        |
| `check_consistency`                                                          | true                           | Vérifie la cohérence des métadonnées locales et des métadonnées dans Keeper, et effectue la récupération de la réplique en cas d’incohérence                                                                                                                                                                                                                                                 |
| `max_retries_before_automatic_recovery`                                      | 10                             | Nombre maximal de tentatives pour exécuter une entrée de la file d’attente avant de marquer la réplique comme perdue et de la récupérer à partir d’un instantané (0 signifie infini)                                                                                                                                                                                                         |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | Si activé, lors du traitement des DDL dans les bases de données Replicated, ignore si possible la création et l’échange des DDL des tables temporaires des vues matérialisées actualisables                                                                                                                                                                                                  |
| `logs_to_keep`                                                               | 1000                           | Nombre par défaut de logs à conserver dans ZooKeeper pour la base de données Replicated.                                                                                                                                                                                                                                                                                                     |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | Le chemin vers la base de données dans ZooKeeper. Utilisé lors de la création de la base de données si les arguments sont omis.                                                                                                                                                                                                                                                              |
| `default_replica_shard_name`                                                 | `{shard}`                      | Le nom du shard de la réplique dans la base de données. Utilisé lors de la création de la base de données si les arguments sont omis.                                                                                                                                                                                                                                                        |
| `default_replica_name`                                                       | `{replica}`                    | Le nom de la réplique dans la base de données. Utilisé lors de la création de la base de données si les arguments sont omis.                                                                                                                                                                                                                                                                 |
| `internal_replication`                                                       | false                          | Indique si une table Distributed créée avec le cluster de cette base de données Replicated enverra les données à l’une des répliques (la réplication interne signifie que les répliques du cluster effectuent elles-mêmes la réplication) ou à toutes les répliques (l’absence de réplication interne signifie que la table Distributed enverra les données insérées à toutes les répliques) |

Les valeurs par défaut peuvent être remplacées dans le fichier de configuration

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```