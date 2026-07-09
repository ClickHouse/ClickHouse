---
description: 'Découvrez comment ajouter une clé de partitionnement personnalisée aux tables MergeTree.'
sidebar_label: 'Clé de partitionnement personnalisée'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: 'Clé de partitionnement personnalisée'
doc_type: 'guide'
---

:::note
Dans la plupart des cas, vous n’avez pas besoin de clé de partition, et dans la plupart des autres, vous n’avez pas besoin d’une clé de partition plus fine qu’un partitionnement mensuel, sauf dans les cas d’usage d’observabilité où un partitionnement journalier est courant.

Vous ne devez jamais utiliser un partitionnement trop fin. Ne partitionnez pas vos données par identifiant ou nom de client. À la place, utilisez l’identifiant ou le nom du client comme première colonne de l’expression ORDER BY.
:::

Le partitionnement est disponible pour les [tables de la famille MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), y compris les [tables répliquées](../../../engines/table-engines/mergetree-family/replication.md) et les [vues matérialisées](/fr/sql-reference/statements/create/view#materialized-view).

Une partition est un regroupement logique d’enregistrements d’une table selon un critère donné. Vous pouvez définir une partition selon n’importe quel critère, par exemple par mois, par jour ou par type d’événement. Chaque partition est stockée séparément afin de simplifier la manipulation de ces données. Lors de l’accès aux données, ClickHouse utilise le plus petit sous-ensemble de partitions possible. Les partitions améliorent les performances des requêtes qui incluent une clé de partitionnement, car ClickHouse filtre d’abord sur cette partition avant de sélectionner les parts et les granules qu’elle contient.

La partition est spécifiée dans la clause `PARTITION BY expr` lors de la [création d’une table](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). La clé de partition peut être n’importe quelle expression basée sur les colonnes de la table. Par exemple, pour définir un partitionnement par mois, utilisez l’expression `toYYYYMM(date_column)` :

```sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

La clé de partitionnement peut également être un tuple d’expressions (comme la [clé primaire](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)). Par exemple :

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

Dans cet exemple, nous définissons le partitionnement en fonction des types d’événements survenus au cours de la semaine en cours.

Par défaut, la clé de partition en virgule flottante n’est pas prise en charge. Pour l’utiliser, activez le paramètre [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key).

Lors de l’insertion de nouvelles données dans une table, celles-ci sont stockées sous la forme d’une part distincte (chunk), triée selon la clé primaire. Dans les 10 à 15 minutes qui suivent l’insertion, les parts de la même partition fusionnent en une seule part.

:::info
Une fusion ne fonctionne que pour les parts de données ayant la même valeur pour l’expression de partitionnement. Cela signifie que **vous ne devez pas créer des partitions trop fines** (plus d’environ mille partitions). Sinon, la requête `SELECT` s’exécute mal en raison d’un nombre excessif de fichiers dans le système de fichiers et de descripteurs de fichiers ouverts.
:::

Utilisez la table [system.parts](../../../operations/system-tables/parts.md) pour afficher les parts de la table et les partitions. Par exemple, supposons que nous ayons une table `visits` avec un partitionnement par mois. Exécutons la requête `SELECT` sur la table `system.parts` :

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

La colonne `partition` contient les noms des partitions. Il y a deux partitions dans cet exemple : `201901` et `201902`. Vous pouvez utiliser la valeur de cette colonne pour spécifier le nom de la partition dans les requêtes [ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md).

La colonne `name` contient les noms des parts de données de la partition. Vous pouvez utiliser cette colonne pour spécifier le nom de la part dans la requête [ALTER ATTACH PART](/fr/sql-reference/statements/alter/partition#attach-partitionpart).

Décomposons le nom de la part : `201901_1_9_2_11` :

* `201901` est le nom de la partition.
* `1` est le numéro minimum du bloc de données.
* `9` est le numéro maximum du bloc de données.
* `2` est le niveau du fragment (la profondeur de l’arbre de fusion dont il est issu).
* `11` est la version de mutation (si une part a subi une mutation)

:::info
Les parts des tables d’ancien type portent le nom suivant : `20190117_20190123_2_2_0` (date minimum - date maximum - numéro minimum de bloc - numéro maximum de bloc - niveau).
:::

La colonne `active` indique l’état de la part. `1` signifie active ; `0`, inactive. Les parts inactives sont, par exemple, des parts source restantes après une fusion vers une part plus grande. Les parts de données corrompues sont également signalées comme inactives.

Comme vous pouvez le voir dans l’exemple, il existe plusieurs parts distinctes de la même partition (par exemple, `201901_1_3_1` et `201901_1_9_2`). Cela signifie que ces parts n’ont pas encore été fusionnées. ClickHouse fusionne périodiquement les parts de données insérées, environ 15 minutes après l’insertion. En outre, vous pouvez effectuer une fusion non planifiée à l’aide de la requête [OPTIMIZE](../../../sql-reference/statements/optimize.md). Exemple :

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

Les parts inactives seront supprimées environ 10 minutes après la fusion.

Une autre façon de voir un ensemble de parts et de partitions consiste à accéder au répertoire de la table : `/var/lib/clickhouse/data/<database>/<table>/`. Par exemple :

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

Les dossiers &#39;201901&#95;1&#95;1&#95;0&#39;, &#39;201901&#95;1&#95;7&#95;1&#39; et ainsi de suite sont les répertoires des parts. Chaque part correspond à une partition et ne contient des données que pour un mois donné (dans cet exemple, la table utilise un partitionnement mensuel).

Le répertoire `detached` contient les parts qui ont été détachées de la table à l’aide de la requête [DETACH](/fr/sql-reference/statements/detach). Les parts corrompues sont également déplacées dans ce répertoire au lieu d’être supprimées. Le serveur n’utilise pas les parts du répertoire `detached`. Vous pouvez ajouter, supprimer ou modifier les données de ce répertoire à tout moment – le serveur n’en sera pas informé tant que vous n’aurez pas exécuté la requête [ATTACH](/fr/sql-reference/statements/alter/partition#attach-partitionpart).

Notez que, sur un serveur en cours de fonctionnement, vous ne pouvez pas modifier manuellement l’ensemble des parts ni leurs données dans le système de fichiers, car le serveur n’en sera pas informé. Pour les tables non répliquées, vous pouvez le faire lorsque le serveur est arrêté, mais ce n’est pas recommandé. Pour les tables répliquées, l’ensemble des parts ne peut en aucun cas être modifié.

ClickHouse vous permet d’effectuer des opérations sur les partitions : les supprimer, copier d’une table vers une autre ou créer une sauvegarde. Consultez la liste complète de ces opérations dans la section [Manipulations des partitions et des parts](/fr/sql-reference/statements/alter/partition).

<div id="group-by-optimisation-using-partition-key">
  ## Optimisation de Group By à l’aide de la clé de partition
</div>

Pour certaines combinaisons entre la clé de partition de la table et la clé de Group By de la requête, il peut être possible d’exécuter l’agrégation indépendamment pour chaque partition.
Nous n’aurons alors pas à fusionner, à la fin, les données partiellement agrégées provenant de tous les threads d’exécution,
car nous avons la garantie que chaque valeur de clé de Group By ne peut pas apparaître dans les ensembles de travail de deux threads différents.

L’exemple typique est :

```sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
Les performances d’une telle requête dépendent fortement de l’organisation de la table. C’est pourquoi cette optimisation n’est pas activée par défaut.
:::

Les facteurs clés pour obtenir de bonnes performances :

* le nombre de partitions impliquées dans la requête doit être suffisamment élevé (plus de `max_threads / 2`), sinon la requête n’utilisera pas pleinement la machine
* les partitions ne doivent pas être trop petites, afin que le traitement par lots ne dégénère pas en traitement ligne par ligne
* les partitions doivent être de taille comparable, afin que tous les threads effectuent approximativement la même quantité de travail

:::info
Il est recommandé d’appliquer une fonction de hachage aux colonnes de la clause `partition by` afin de répartir uniformément les données entre les partitions.
:::

Les paramètres pertinents sont :

* `allow_aggregate_partitions_independently` - détermine si l’utilisation de cette optimisation est activée
* `force_aggregate_partitions_independently` - force son utilisation lorsqu’elle est applicable du point de vue de la validité du résultat, mais désactivée par la logique interne qui en évalue la pertinence
* `max_number_of_partitions_for_independent_aggregation` - limite stricte du nombre maximal de partitions que la table peut avoir