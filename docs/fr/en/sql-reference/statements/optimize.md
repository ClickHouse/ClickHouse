---
description: 'Documentation pour OPTIMIZE'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'Instruction OPTIMIZE'
doc_type: 'reference'
---

Cette requête tente de déclencher une fusion non planifiée de parts de données dans les tables. Notez que nous déconseillons généralement l&#39;utilisation de `OPTIMIZE TABLE ... FINAL` (voir cette [documentation](/fr/optimize/avoidoptimizefinal)), car elle est destinée à l&#39;administration, et non à une utilisation quotidienne.

:::note
`OPTIMIZE` ne peut pas corriger l&#39;erreur `Too many parts`.
:::

**Syntaxe**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

La requête `OPTIMIZE` est prise en charge pour la famille [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (y compris les [vues matérialisées](/fr/sql-reference/statements/create/view#materialized-view)) et les moteurs [Buffer](../../engines/table-engines/special/buffer.md). Les autres moteurs de table ne sont pas pris en charge.

Lorsque `OPTIMIZE` est utilisé avec la famille de moteurs de table [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md), ClickHouse crée une tâche de fusion et attend son exécution sur toutes les répliques (si le paramètre [alter&#95;sync](/fr/operations/settings/settings#alter_sync) est défini sur `2`) ou sur la réplique actuelle (si le paramètre [alter&#95;sync](/fr/operations/settings/settings#alter_sync) est défini sur `1`).

* Si `OPTIMIZE` n&#39;effectue aucune fusion, quelle qu&#39;en soit la raison, le client n&#39;en est pas informé. Pour activer les notifications, utilisez le paramètre [optimize&#95;throw&#95;if&#95;noop](/fr/operations/settings/settings#optimize_throw_if_noop).
* Si vous spécifiez une `PARTITION`, seule la partition indiquée est optimisée. [Comment définir une expression de partition](alter/partition.md#how-to-set-partition-expression).
* Si vous spécifiez `FINAL` ou `FORCE`, l&#39;optimisation est effectuée même lorsque toutes les données se trouvent déjà dans une seule part. Vous pouvez contrôler ce comportement avec [optimize&#95;skip&#95;merged&#95;partitions](/fr/operations/settings/settings#optimize_skip_merged_partitions). De plus, la fusion est forcée même si des fusions concurrentes sont en cours.
* Si vous spécifiez `DEDUPLICATE`, les lignes strictement identiques (sauf si une clause BY est spécifiée) seront dédupliquées (toutes les colonnes sont comparées) ; cela n&#39;a de sens que pour le moteur MergeTree.

Vous pouvez définir combien de temps (en secondes) attendre que des répliques inactives exécutent les requêtes `OPTIMIZE` à l&#39;aide du paramètre [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/fr/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Si `alter_sync` est défini sur `2` et que certaines répliques restent inactives au-delà du délai spécifié par le paramètre `replication_wait_for_inactive_replica_timeout`, une exception `UNFINISHED` est levée.
:::

<div id="dry-run">
  ## DRY RUN
</div>

La clause `DRY RUN` simule une fusion des parts spécifiées sans rendre le résultat définitif. La part fusionnée est écrite dans un emplacement temporaire, vérifiée, puis supprimée. Les parts d’origine et les données de la table restent inchangées.

Cela est utile pour :

* Tester le bon déroulement des fusions entre différentes versions de ClickHouse.
* Reproduire de manière déterministe des bogues liés aux fusions.
* Mesurer les performances des fusions.

`DRY RUN` n’est pris en charge que pour les tables de la famille [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Le mot-clé `PARTS` accompagné d’une liste de noms de parts est requis. Toutes les parts spécifiées doivent exister, être actives et appartenir à la même partition.

`DRY RUN` est incompatible avec `FINAL` et `PARTITION`. Il peut être combiné avec `DEDUPLICATE` (avec spécification facultative des colonnes) et `CLEANUP` (pour les tables `ReplacingMergeTree`).

**Syntaxe**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

Par défaut, la part fusionnée obtenue est validée de manière similaire à la requête [`CHECK TABLE`](/fr/sql-reference/statements/check-table). Ce comportement est contrôlé par le paramètre [optimize&#95;dry&#95;run&#95;check&#95;part](/fr/operations/settings/settings#optimize_dry_run_check_part) (activé par défaut). Le désactiver permet d’ignorer la validation, ce qui peut être utile pour mesurer les performances de la fusion elle-même.

**Exemple**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## Expression BY
</div>

Si vous souhaitez effectuer une déduplication sur un ensemble personnalisé de colonnes plutôt que sur toutes les colonnes, vous pouvez spécifier explicitement une liste de colonnes ou utiliser n’importe quelle combinaison d’expressions [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](/fr/sql-reference/statements/select#select-clause) ou [`EXCEPT`](/fr/sql-reference/statements/select/except-modifier). La liste de colonnes explicitement indiquée ou implicitement développée doit inclure toutes les colonnes spécifiées dans l’expression d’ordre des lignes (à la fois la clé primaire et la clé de tri) ainsi que dans l’expression de partitionnement (clé de partitionnement).

:::note
Notez que `*` se comporte comme dans `SELECT` : les colonnes [MATERIALIZED](/fr/sql-reference/statements/create/view#materialized-view) et [ALIAS](../../sql-reference/statements/create/table.md#alias) ne sont pas prises en compte dans l’expansion.

De plus, spécifier une liste vide de colonnes, écrire une expression qui produit une liste vide de colonnes ou effectuer la déduplication sur une colonne `ALIAS` provoque une erreur.
:::

**Syntaxe**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**Exemples**

Considérez la table suivante :

```sql title="Query"
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Tous les exemples ci-dessous sont exécutés à partir de cet état de 5 lignes.

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

Lorsque les colonnes de déduplication ne sont pas précisées, elles sont toutes prises en compte. La ligne n’est supprimée que si toutes les valeurs de toutes les colonnes sont égales aux valeurs correspondantes de la ligne précédente :

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

Lorsque les colonnes sont implicites, la table est dédupliquée sur toutes les colonnes qui ne sont ni `ALIAS` ni `MATERIALIZED`. Dans l’exemple de table ci-dessus, il s’agit des colonnes `primary_key`, `secondary_key`, `value` et `partition_key` :

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

Dédupliquez en fonction de toutes les colonnes qui ne sont ni `ALIAS` ni `MATERIALIZED`, en excluant explicitement `value` : les colonnes `primary_key`, `secondary_key` et `partition_key`.

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

Dédupliquez explicitement à l’aide des colonnes `primary_key`, `secondary_key` et `partition_key` :

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

Dédupliquez à l’aide de toutes les colonnes correspondant à une expression régulière : les colonnes `primary_key`, `secondary_key` et `partition_key` :

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```