---
description: 'Documentation sur la manipulation des indices de saut de données'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'Manipulation des indices de saut de données'
toc_hidden_folder: true
doc_type: 'reference'
---

Les opérations suivantes sont disponibles :

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - Ajoute la description de l’index dans les métadonnées de la table.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - Supprime la description des métadonnées de la table et supprime les fichiers d&#39;index du disque. Implémenté sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Reconstruit l’index secondaire `name` pour la partition `partition_name` spécifiée. Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations). Si la clause `IN PARTITION` est omise, l’index est reconstruit pour l’ensemble des données de la table.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Supprime du disque les fichiers d’index secondaires sans en supprimer la description. Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

Les commandes `ADD`, `DROP` et `CLEAR` sont légères, au sens où elles ne modifient que les métadonnées ou suppriment des fichiers.
De plus, elles sont répliquées, les métadonnées des index étant synchronisées via ClickHouse Keeper ou ZooKeeper.

:::note
La manipulation des index n’est prise en charge que pour les tables utilisant le moteur [`*MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree.md) (y compris les variantes [répliquées](/fr/engines/table-engines/mergetree-family/replication.md)).
:::