---
description: "Documentation sur la modification de l’expression SAMPLE BY"
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: "Modification des expressions de clé d’échantillonnage"
doc_type: 'reference'
---

Les opérations suivantes sont disponibles :

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

La commande remplace la [clé d&#39;échantillonnage](../../../engines/table-engines/mergetree-family/mergetree.md) de la table par `new_expression` (une expression ou un tuple d&#39;expressions). La clé primaire doit contenir la nouvelle clé d&#39;échantillonnage.

<div id="remove">
  ## SUPPRIMER
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

La commande supprime la [clé d&#39;échantillonnage](../../../engines/table-engines/mergetree-family/mergetree.md) de la table.

Les commandes `MODIFY` et `REMOVE` sont légères, dans la mesure où elles se contentent de modifier les métadonnées ou de supprimer des fichiers.

:::note
Elle fonctionne uniquement pour les tables de la famille [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les tables [répliquées](../../../engines/table-engines/mergetree-family/replication.md)).
:::