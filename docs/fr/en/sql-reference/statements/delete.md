---
description: 'Le `DELETE` léger simplifie le processus de suppression des données de la base de données.'
keywords: ['delete']
sidebar_label: 'DELETE'
sidebar_position: 36
slug: /sql-reference/statements/delete
title: "L'instruction `DELETE` légère"
doc_type: 'reference'
---

L&#39;instruction `DELETE` légère supprime les lignes de la table `[db.]table` qui correspondent à l&#39;expression `expr`. Elle n&#39;est disponible que pour la famille de moteurs de table *MergeTree.

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

On l’appelle &quot;`DELETE` léger&quot; par opposition à la commande [ALTER TABLE ... DELETE](/fr/sql-reference/statements/alter/delete), qui est une opération lourde.

<div id="examples">
  ## Exemples
</div>

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

<div id="lightweight-delete-does-not-delete-data-immediately">
  ## Le `DELETE` léger ne supprime pas les données immédiatement
</div>

Le `DELETE` léger est implémenté sous forme de [mutation](/fr/sql-reference/statements/alter#mutations) qui marque les lignes comme supprimées, sans toutefois les supprimer physiquement immédiatement.

Par défaut, les instructions `DELETE` attendent que le marquage des lignes comme supprimées soit terminé avant de se terminer. Cela peut prendre beaucoup de temps si le volume de données est important. Vous pouvez aussi l’exécuter de manière asynchrone en arrière-plan à l’aide du paramètre [`lightweight_deletes_sync`](/fr/operations/settings/settings#lightweight_deletes_sync). S’il est désactivé, l’instruction `DELETE` se termine immédiatement, mais les données peuvent encore rester visibles pour les requêtes jusqu’à la fin de la mutation en arrière-plan.

La mutation ne supprime pas physiquement les lignes qui ont été marquées comme supprimées ; cela ne se produira qu’au moment de la prochaine fusion. Par conséquent, il est possible que, pendant une durée indéterminée, les données ne soient pas réellement supprimées du stockage et soient seulement marquées comme supprimées.

Si vous devez garantir que vos données sont supprimées du stockage dans un délai prévisible, envisagez d’utiliser le paramètre de table [`min_age_to_force_merge_seconds`](/fr/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds). Vous pouvez également utiliser la commande [ALTER TABLE ... DELETE](/fr/sql-reference/statements/alter/delete). Notez que la suppression de données avec `ALTER TABLE ... DELETE` peut consommer des ressources importantes, car elle recrée toutes les `parts` affectées.

<div id="deleting-large-amounts-of-data">
  ## Suppression de grandes quantités de données
</div>

Les suppressions massives peuvent nuire aux performances de ClickHouse. Si vous cherchez à supprimer toutes les lignes d’une table, envisagez d’utiliser la commande [`TRUNCATE TABLE`](/fr/sql-reference/statements/truncate).

Si vous prévoyez des suppressions fréquentes, envisagez d’utiliser une [clé de partitionnement personnalisée](/fr/engines/table-engines/mergetree-family/custom-partitioning-key). Vous pourrez alors utiliser la commande [`ALTER TABLE ... DROP PARTITION`](/fr/sql-reference/statements/alter/partition#drop-partitionpart) pour supprimer rapidement toutes les lignes associées à cette partition.

<div id="limitations-of-lightweight-delete">
  ## Limites du `DELETE` léger
</div>

<div id="lightweight-deletes-with-projections">
  ### `DELETE` légers avec des projections
</div>

Par défaut, `DELETE` ne fonctionne pas pour les tables avec des projections. Cela s’explique par le fait que des lignes d’une projection peuvent être affectées par une opération `DELETE`. Il existe toutefois un [paramètre MergeTree](/fr/operations/settings/merge-tree-settings), `lightweight_mutation_projection_mode`, qui permet de modifier ce comportement.

<div id="performance-considerations-when-using-lightweight-delete">
  ## Considérations relatives aux performances lors de l&#39;utilisation de `DELETE` léger
</div>

**La suppression de gros volumes de données avec l&#39;instruction `DELETE` léger peut dégrader les performances des requêtes SELECT.**

Les éléments suivants peuvent également dégrader les performances de `DELETE` léger :

* Une condition `WHERE` complexe dans une requête `DELETE`.
* Si la file d&#39;attente des mutations contient déjà de nombreuses autres mutations, cela peut entraîner des problèmes de performances, car toutes les mutations d&#39;une table sont exécutées séquentiellement.
* La table concernée comporte un très grand nombre de data parts.
* Une grande quantité de données dans des compact parts. Dans une Compact part, toutes les colonnes sont stockées dans un seul fichier.

<div id="delete-permissions">
  ## Autorisations de suppression
</div>

`DELETE` nécessite le privilège `ALTER DELETE`. Pour autoriser les instructions `DELETE` sur une table donnée pour un utilisateur spécifique, exécutez la commande suivante :

```sql
GRANT ALTER DELETE ON db.table to username;
```

<div id="how-lightweight-deletes-work-internally-in-clickhouse">
  ## Fonctionnement interne des `DELETE` légers dans ClickHouse
</div>

1. **Un « masque » est appliqué aux lignes affectées**

   Lorsqu’une requête `DELETE FROM table ...` est exécutée, ClickHouse enregistre un masque dans lequel chaque ligne est marquée soit comme « existante », soit comme « supprimée ». Ces lignes « supprimées » sont exclues des requêtes suivantes. Toutefois, les lignes ne sont réellement supprimées que plus tard, lors des fusions ultérieures. L’écriture de ce masque est bien plus lightweight que ce qui est effectué par une requête `ALTER TABLE ... DELETE`.

   Le masque est implémenté sous la forme d’une colonne système cachée `_row_exists`, qui stocke `True` pour toutes les lignes visibles et `False` pour les lignes supprimées. Cette colonne n’est présente dans une part que si certaines lignes de cette part ont été supprimées. Elle n’existe pas lorsqu’une part a toutes ses valeurs égales à `True`.

2. **Les requêtes `SELECT` sont transformées pour inclure le masque**

   Lorsqu’une colonne masquée est utilisée dans une requête, la requête `SELECT ... FROM table WHERE condition` est étendue en interne avec le prédicat sur `_row_exists` et transformée en :

   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```

   Au moment de l’exécution, la colonne `_row_exists` est lue pour déterminer quelles lignes ne doivent pas être renvoyées. S’il y a beaucoup de lignes supprimées, ClickHouse peut déterminer quels granules peuvent être entièrement ignorés lors de la lecture du reste des colonnes.

3. **Les requêtes `DELETE` sont transformées en requêtes `ALTER TABLE ... UPDATE`**

   La requête `DELETE FROM table WHERE condition` est traduite en mutation `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition`.

   En interne, cette mutation est exécutée en deux étapes :

   1. Une commande `SELECT count() FROM table WHERE condition` est exécutée pour chaque part afin de déterminer si elle est affectée.

   2. Sur la base des commandes ci-dessus, les parts affectées sont ensuite mutées, et des hardlinks sont créés pour les parts non affectées. Dans le cas des wide parts, la colonne `_row_exists` de chaque ligne est mise à jour, et les fichiers de toutes les autres colonnes sont conservés via hardlinks. Pour les compact parts, toutes les colonnes sont réécrites, car elles sont toutes stockées ensemble dans un seul fichier.

   Comme le montrent les étapes ci-dessus, le `DELETE` léger utilisant la technique du masquage offre de meilleures performances que le `ALTER TABLE ... DELETE` traditionnel, car il ne réécrit pas les fichiers de toutes les colonnes pour les parts affectées.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Gérer les mises à jour et les suppressions dans ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)