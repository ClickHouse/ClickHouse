---
description: 'Documentation sur l’application du masque des lignes supprimées'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'Application du masque des lignes supprimées'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

La commande applique le masque créé par une [suppression légère](/fr/sql-reference/statements/delete) et supprime de force du disque les lignes marquées comme supprimées. Cette commande est une mutation lourde et équivaut sémantiquement à la requête `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`.

:::note
Elle fonctionne uniquement pour les tables de la famille [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les tables [répliquées](../../../engines/table-engines/mergetree-family/replication.md)).
:::

**Voir aussi**

* [Suppressions légères](/fr/sql-reference/statements/delete)
* [Suppressions lourdes](/fr/sql-reference/statements/alter/delete.md)