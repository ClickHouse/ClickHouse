---
description: 'Documentation sur la modification des expressions de clé'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: 'Modification des expressions de clé'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

La commande modifie la [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md) de la table en `new_expression` (une expression ou un tuple d&#39;expressions). La clé primaire reste inchangée.

Cette commande est légère, dans la mesure où elle ne modifie que les métadonnées. Pour préserver le fait que les lignes des parties de données sont ordonnées selon l&#39;expression de la clé de tri, vous ne pouvez pas ajouter à la clé de tri des expressions contenant des colonnes existantes (seules les colonnes ajoutées par la commande `ADD COLUMN` dans la même requête `ALTER`, sans valeur par défaut pour la colonne, sont autorisées).

:::note
Cela fonctionne uniquement pour les tables de la famille [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les tables [répliquées](../../../engines/table-engines/mergetree-family/replication.md)).
:::