---
description: 'Documentation de APPLY PATCHES pour les lightweight updates'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'APPLY PATCHES pour les lightweight updates'
doc_type: 'référence'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

La commande déclenche manuellement la matérialisation physique des patch parts créées par les instructions [lightweight `UPDATE`](/fr/sql-reference/statements/update). Elle applique de manière forcée les patches en attente aux data parts en réécrivant uniquement les colonnes concernées.

:::note

* Elle fonctionne uniquement pour les tables de la famille [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les tables [répliquées](../../../engines/table-engines/mergetree-family/replication.md)).
* Il s&#39;agit d&#39;une opération de mutation qui s&#39;exécute de manière asynchrone en arrière-plan.
  :::

<div id="when-to-use">
  ## Quand utiliser APPLY PATCHES
</div>

:::tip
En général, vous ne devriez pas avoir besoin d&#39;utiliser `APPLY PATCHES`
:::

Les patch parts sont normalement appliquées automatiquement lors des fusions lorsque le paramètre [`apply_patches_on_merge`](/fr/operations/settings/merge-tree-settings#apply_patches_on_merge) est activé (par défaut). Cependant, vous pouvez souhaiter déclencher manuellement l&#39;application des patchs dans les cas suivants :

* Réduire la surcharge liée à l&#39;application des patchs lors des requêtes `SELECT`
* Consolider plusieurs patch parts avant qu&#39;elles ne s&#39;accumulent
* Préparer les données pour une sauvegarde ou une exportation avec les patchs déjà matérialisés
* Lorsque `apply_patches_on_merge` est désactivé et que vous souhaitez contrôler le moment où les patchs sont appliqués

<div id="examples">
  ## Exemples
</div>

Appliquez tous les patchs en attente d’une table :

```sql
ALTER TABLE my_table APPLY PATCHES;
```

Appliquez les patchs uniquement à une partition spécifique :

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Combinez avec d’autres opérations :

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## Suivi de l’application des patchs
</div>

Vous pouvez suivre l’avancement de l’application des patchs à l’aide de la table [`system.mutations`](/fr/operations/system-tables/mutations) :

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## Voir aussi
</div>

* [Lightweight `UPDATE`](/fr/sql-reference/statements/update) - Créer des patch parts à l’aide de lightweight updates
* [paramètre `apply_patches_on_merge`](/fr/operations/settings/merge-tree-settings#apply_patches_on_merge) - Contrôler l’application automatique des patches lors des fusions