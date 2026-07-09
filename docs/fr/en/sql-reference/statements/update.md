---
description: 'Les mises à jour légères simplifient la mise à jour des données dans la base de données à l’aide de patch parts.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: "L'instruction UPDATE légère"
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
Les mises à jour légères sont actuellement en bêta.
Si vous rencontrez des problèmes, veuillez ouvrir un ticket dans le [dépôt ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

L&#39;instruction légère `UPDATE` met à jour les lignes d&#39;une table `[db.]table` qui correspondent à l&#39;expression `filter_expr`.
On parle de « mise à jour légère » par opposition à la requête [`ALTER TABLE ... UPDATE`](/fr/sql-reference/statements/alter/update), qui est un processus lourd réécrivant des colonnes entières dans les parties de données.
Elle n&#39;est disponible que pour la famille de moteurs de table [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree).

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

Le `filter_expr` doit être de type `UInt8`. Cette requête met à jour les valeurs des colonnes spécifiées en leur attribuant les valeurs des expressions correspondantes dans les lignes pour lesquelles `filter_expr` prend une valeur non nulle.
Les valeurs sont converties dans le type de la colonne à l’aide de l’opérateur `CAST`. La mise à jour des colonnes utilisées dans le calcul des clés primaires ou de partition n’est pas prise en charge.

<div id="examples">
  ## Exemples
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## Les mises à jour légères ne mettent pas à jour les données immédiatement
</div>

Le `UPDATE` léger est implémenté à l’aide de **patch parts** — un type spécial de partie de données qui contient uniquement les colonnes et les lignes mises à jour.
Un `UPDATE` léger crée des patch parts, mais ne modifie pas immédiatement les données d’origine de manière physique dans le stockage.
Le processus de mise à jour est similaire à une requête `INSERT ... SELECT ...`, mais la requête `UPDATE` attend que la création de la patch part soit terminée avant de renvoyer.

Les valeurs mises à jour sont :

* **Immédiatement visibles** dans les requêtes `SELECT` grâce à l’application des patchs
* **Physiquement matérialisées** uniquement lors des fusions et mutations ultérieures
* **Automatiquement supprimées** une fois que toutes les parties actives ont leurs patchs matérialisés

<div id="lightweight-update-requirements">
  ## Exigences pour les mises à jour légères
</div>

Les mises à jour légères sont prises en charge par les moteurs [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/fr/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/fr/engines/table-engines/mergetree-family/collapsingmergetree), [`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree), ainsi que par leurs versions [`Replicated`](/fr/engines/table-engines/mergetree-family/replication.md) et [`Shared`](/fr/cloud/reference/shared-merge-tree).

Pour utiliser les mises à jour légères, la matérialisation des colonnes `_block_number` et `_block_offset` doit être activée à l’aide des paramètres de table [`enable_block_number_column`](/fr/operations/settings/merge-tree-settings#enable_block_number_column) et [`enable_block_offset_column`](/fr/operations/settings/merge-tree-settings#enable_block_offset_column).

<div id="lightweight-delete">
  ## Suppressions légères
</div>

Une requête [lightweight `DELETE`](/fr/sql-reference/statements/delete) peut être exécutée sous la forme d’un `UPDATE` léger plutôt que comme une mutation `ALTER UPDATE`. L’implémentation de `lightweight `DELETE&#96;&#96; est contrôlée par le paramètre [`lightweight_delete_mode`](/fr/operations/settings/settings#lightweight_delete_mode).

<div id="performance-considerations">
  ## Considérations en matière de performances
</div>

**Avantages des mises à jour légères :**

* La latence de la mise à jour est comparable à celle de la requête `INSERT ... SELECT ...`
* Seules les colonnes et les valeurs mises à jour sont écrites, et non des colonnes entières dans les partie de données
* Il n&#39;est pas nécessaire d&#39;attendre la fin des fusions/mutations en cours. La latence d&#39;une mise à jour est donc prévisible
* L&#39;exécution parallèle des mises à jour légères est possible

**Impacts potentiels sur les performances :**

* Ajoute une surcharge aux requêtes `SELECT` qui doivent appliquer des patchs
* Les [index de saut](/fr/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) ne seront pas utilisés pour les colonnes des partie de données auxquelles des patchs doivent être appliqués. Les [projections](/fr/engines/table-engines/mergetree-family/mergetree.md/#projections) ne seront pas utilisées s&#39;il existe des patch parts pour la table, y compris pour les partie de données auxquelles aucun patch ne doit être appliqué.
* De petites mises à jour trop fréquentes peuvent entraîner une erreur « too many parts ». Il est recommandé de regrouper plusieurs mises à jour dans une seule requête, par exemple en plaçant les identifiants à mettre à jour dans une seule clause `IN` de la clause `WHERE`
* Les mises à jour légères sont conçues pour mettre à jour de petites quantités de lignes (jusqu&#39;à environ 10 % de la table). Si vous devez mettre à jour un volume plus important, il est recommandé d&#39;utiliser la mutation [`ALTER TABLE ... UPDATE`](/fr/sql-reference/statements/alter/update)

<div id="concurrent-operations">
  ## Opérations concurrentes
</div>

Contrairement aux mutations lourdes, les mises à jour légères n&#39;attendent pas la fin des fusions/mutations en cours.
La cohérence des mises à jour légères concurrentes est contrôlée par les paramètres [`update_sequential_consistency`](/fr/operations/settings/settings#update_sequential_consistency) et [`update_parallel_mode`](/fr/operations/settings/settings#update_parallel_mode).

<div id="update-permissions">
  ## Autorisations pour UPDATE
</div>

`UPDATE` nécessite le privilège `ALTER UPDATE`. Pour autoriser les instructions `UPDATE` sur une table spécifique pour un utilisateur donné, exécutez :

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## Détails de l’implémentation
</div>

Les patch parts sont identiques aux parts ordinaires, mais elles ne contiennent que les colonnes mises à jour ainsi que plusieurs colonnes système :

* `_part` - le nom de la part d’origine
* `_part_offset` - le numéro de ligne dans la part d’origine
* `_block_number` - le numéro du block de la ligne dans la part d’origine
* `_block_offset` - le décalage du block de la ligne dans la part d’origine
* `_data_version` - la version des données mises à jour (numéro de block alloué à la requête `UPDATE`)

En moyenne, cela représente environ 40 octets de surcoût (données non compressées) par ligne mise à jour dans les patch parts.
Les colonnes système aident à retrouver les lignes de la part d’origine qui doivent être mises à jour.
Les colonnes système sont liées aux [colonnes virtuelles](/fr/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) de la part d’origine, qui sont ajoutées à la lecture lorsque des patch parts doivent être appliquées.
Les patch parts sont triées par `_part` et `_part_offset`.

Les patch parts appartiennent à des partitions différentes de celle de la part d’origine.
L’identifiant de partition de la patch part est `patch-<hash of column names in patch part>-<original_partition_id>`.
Par conséquent, les patch parts contenant des colonnes différentes sont stockées dans des partitions différentes.
Par exemple, trois mises à jour `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>` et `SET x = 1, y = 1 WHERE <cond>` créeront trois patch parts dans trois partitions différentes.

Les patch parts peuvent être fusionnées entre elles afin de réduire le nombre de patchs appliqués aux requêtes `SELECT` et de réduire le surcoût. La fusion des patch parts utilise l’algorithme de fusion [replacing](/fr/engines/table-engines/mergetree-family/replacingmergetree) avec `_data_version` comme colonne de version.
Par conséquent, les patch parts stockent toujours la version la plus récente de chaque ligne mise à jour dans la part.

Les lightweight updates n’attendent pas la fin des fusions et des mutations en cours et utilisent toujours un snapshot actuel des partie de données pour exécuter une mise à jour et produire une patch part.
De ce fait, il peut y avoir deux cas lors de l’application des patch parts.

Par exemple, si nous lisons la part `A`, nous devons appliquer la patch part `X` :

* si `X` contient la part `A` elle-même. Cela se produit si `A` ne participait pas à une fusion lorsque `UPDATE` a été exécuté.
* si `X` contient les parts `B` et `C`, qui sont couvertes par la part `A`. Cela se produit si une fusion (`B`, `C`) -&gt; `A` était en cours lorsque `UPDATE` a été exécuté.

Pour ces deux cas, il existe respectivement deux façons d’appliquer les patch parts :

* En utilisant une fusion sur les colonnes triées `_part`, `_part_offset`.
* En utilisant un join sur les colonnes `_block_number`, `_block_offset`.

Le mode join est plus lent et nécessite plus de mémoire que le mode fusion, mais il est utilisé plus rarement.

<div id="related-content">
  ## Contenu connexe
</div>

* [`ALTER UPDATE`](/fr/sql-reference/statements/alter/update) - Opérations `UPDATE` coûteuses
* [Lightweight `DELETE`](/fr/sql-reference/statements/delete) - Opérations `DELETE` légères
* [`APPLY PATCHES`](/fr/sql-reference/statements/alter/apply-patches) - Forcer la matérialisation physique des patches dans les parties de données (opération de mutation)