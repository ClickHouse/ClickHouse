---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "Cl\xE9 De Partitionnement Personnalis\xE9e"
---

# Clé De Partitionnement Personnalisée {#custom-partitioning-key}

Le partitionnement est disponible pour [MergeTree](mergetree.md) table de famille (y compris les [répliqué](replication.md) table). [Les vues matérialisées](../special/materializedview.md#materializedview) basé sur les tables MergeTree prennent également en charge le partitionnement.

Une partition est une combinaison logique d'enregistrements dans une table selon un critère spécifié. Vous pouvez définir une partition par un critère arbitraire, comme, par mois, par jour, ou par type d'événement. Chaque partition est stockée séparément pour simplifier les manipulations de ces données. Lors de l'accès aux données, ClickHouse utilise le plus petit sous-ensemble de partitions possible.

La partition est spécifiée dans le `PARTITION BY expr` clause lors de [création d'une table](mergetree.md#table_engine-mergetree-creating-a-table). La clé de partition peut être n'importe quelle expression des colonnes de la table. Par exemple, pour spécifier le partitionnement par mois, utilisez l'expression `toYYYYMM(date_column)`:

``` sql
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

La clé de partition peut également être un tuple d'expressions (similaire à la [clé primaire](mergetree.md#primary-keys-and-indexes-in-queries)). Exemple:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

Dans cet exemple, nous définissons le partitionnement par les types d'événements qui se sont produits au cours de la semaine en cours.

Lors de l'insertion de nouvelles données dans une table, ces données sont stockées en tant que partie séparée (bloc) triée par la clé primaire. Dans 10-15 minutes après l'insertion, les parties de la même partition sont fusionnées dans la partie entière.

!!! info "Info"
    Une fusion ne fonctionne que pour les parties de données qui ont la même valeur pour l'expression de partitionnement. Cela signifie **vous ne devriez pas faire des partitions trop granulaires** (plus d'un millier de partitions). Sinon, l' `SELECT` la requête fonctionne mal en raison d'un nombre déraisonnablement élevé de fichiers dans le système de fichiers et des descripteurs de fichiers ouverts.

L'utilisation de la [système.partie](../../../operations/system-tables.md#system_tables-parts) table pour afficher les parties et les partitions de la table. Par exemple, supposons que nous avons une `visits` table avec partitionnement par mois. Nous allons effectuer le `SELECT` la requête pour l' `system.parts` table:

``` sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

Le `partition` colonne contient les noms des partitions. Il y a deux partitions dans cet exemple: `201901` et `201902`. Vous pouvez utiliser cette valeur de colonne pour spécifier le nom de [ALTER … PARTITION](#alter_manipulations-with-partitions) requête.

Le `name` colonne contient les noms des parties de données de partition. Vous pouvez utiliser cette colonne pour spécifier le nom de la partie dans la [ALTER ATTACH PART](#alter_attach-partition) requête.

Décomposons le nom de la première partie: `201901_1_3_1`:

-   `201901` est le nom de la partition.
-   `1` est le nombre minimum du bloc de données.
-   `3` est le nombre maximal de blocs de données.
-   `1` est le niveau de bloc (la profondeur de l'arbre de fusion à partir duquel il est formé).

!!! info "Info"
    Les parties des tables de type ancien ont le nom: `20190117_20190123_2_2_0` (date minimale - date maximale - numéro de bloc minimum - numéro de bloc maximum-niveau).

Le `active` colonne indique le statut de la partie. `1` est active; `0` est inactif. Les parties inactives sont, par exemple, des parties source restant après la fusion à une partie plus grande. Les parties de données corrompues sont également indiquées comme inactives.

Comme vous pouvez le voir dans l'exemple, il y a plusieurs parties séparées de la même partition (par exemple, `201901_1_3_1` et `201901_1_9_2`). Cela signifie que ces parties ne sont pas encore fusionnées. Clickhouse fusionne les parties insérées des données périodiquement, environ 15 minutes après l'insertion. En outre, vous pouvez effectuer une fusion non planifiée en utilisant [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) requête. Exemple:

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

Les parties inactives seront supprimées environ 10 minutes après la fusion.

Une autre façon d'afficher un ensemble de pièces et de partitions est d'aller dans le répertoire de la table: `/var/lib/clickhouse/data/<database>/<table>/`. Exemple:

``` bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

Dossier ‘201901\_1\_1\_0’, ‘201901\_1\_7\_1’ et ainsi de suite sont les répertoires des parties. Chaque partie se rapporte à une partition correspondante et contient des données juste pour un certain mois (la table dans cet exemple a partitionnement par mois).

Le `detached` le répertoire contient des parties qui ont été détachées de la table à l'aide [DETACH](../../../sql-reference/statements/alter.md#alter_detach-partition) requête. Les parties corrompues sont également déplacées dans ce répertoire, au lieu d'être supprimées. Le serveur n'utilise pas les pièces de la `detached` directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you run the [ATTACH](../../../sql-reference/statements/alter.md#alter_attach-partition) requête.

Notez que sur le serveur d'exploitation, vous ne pouvez pas modifier manuellement l'ensemble de pièces ou leurs données sur le système de fichiers, car le serveur ne le saura pas. Pour les tables non répliquées, vous pouvez le faire lorsque le serveur est arrêté, mais ce n'est pas recommandé. Pour les tables répliquées, l'ensemble de pièces ne peut en aucun cas être modifié.

ClickHouse vous permet d'effectuer des opérations avec les partitions: les supprimer, copier d'une table à une autre, ou créer une sauvegarde. Voir la liste de toutes les opérations de la section [Manipulations avec des Partitions et des pièces](../../../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
