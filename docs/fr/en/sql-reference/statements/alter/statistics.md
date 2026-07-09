---
description: 'Documentation sur la manipulation des statistiques de colonnes'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'manipulation des statistiques de colonnes'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # Manipulation des statistiques de colonnes
</div>

<CloudNotSupportedBadge />

Les opérations suivantes sont disponibles :

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Ajoute une description des statistiques aux métadonnées des tables.

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Modifie la description des statistiques dans les métadonnées des tables.

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Supprime les statistiques des métadonnées des colonnes spécifiées et supprime tous les objets de statistiques dans toutes les parts pour ces colonnes.

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Supprime tous les objets de statistiques dans toutes les parts pour les colonnes spécifiées. Les objets de statistiques peuvent être reconstruits à l&#39;aide de `ALTER TABLE MATERIALIZE STATISTICS`.

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - Reconstruit les statistiques des colonnes. Cette opération est implémentée sous la forme d&#39;une [mutation](../../../sql-reference/statements/alter/index.md#mutations).

Les deux premières commandes sont légères, dans le sens où elles ne font que modifier les métadonnées ou supprimer des fichiers.

Elles sont également répliquées, avec synchronisation des métadonnées des statistiques via ZooKeeper.

<div id="example">
  ## Exemple :
</div>

Ajout de deux types de statistiques sur deux colonnes :

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
Les statistiques sont prises en charge uniquement pour les tables utilisant le moteur [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les variantes [répliquées](../../../engines/table-engines/mergetree-family/replication.md)).
:::