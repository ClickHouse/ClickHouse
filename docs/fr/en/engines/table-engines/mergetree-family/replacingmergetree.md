---
description: 'diffère de MergeTree en ce qu''il supprime les entrées en double ayant la
  même valeur de clé de tri (section `ORDER BY` de la table, et non `PRIMARY KEY`).'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'Moteur de table ReplacingMergeTree'
doc_type: 'référence'
---

Ce moteur diffère de [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree) en ce qu&#39;il supprime les entrées en double ayant la même valeur de [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md) (section `ORDER BY` de la table, et non `PRIMARY KEY`).

La déduplication des données ne se produit qu&#39;au moment d&#39;une fusion. Les fusions s&#39;exécutent en arrière-plan à un moment indéterminé, vous ne pouvez donc pas les planifier. Il se peut qu&#39;une partie des données reste non traitée. Bien que vous puissiez lancer une fusion non planifiée à l&#39;aide de la requête `OPTIMIZE`, ne comptez pas dessus, car la requête `OPTIMIZE` lira et écrira une grande quantité de données.

Ainsi, `ReplacingMergeTree` convient pour éliminer les données en double en arrière-plan afin d&#39;économiser de l&#39;espace, mais ne garantit pas l&#39;absence de doublons.

:::note
Un guide détaillé sur ReplacingMergeTree, comprenant les bonnes pratiques et la manière d&#39;optimiser les performances, est disponible [ici](/fr/guides/replacing-merge-tree).
:::

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de la requête, consultez la [description de l’instruction](../../../sql-reference/statements/create/table.md).

:::note
L’unicité des lignes est déterminée par la section `ORDER BY` de la table, et non par `PRIMARY KEY`.
:::

<div id="replacingmergetree-parameters">
  ## Paramètres de ReplacingMergeTree
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — colonne contenant le numéro de version. Type `UInt*`, `Date`, `DateTime` ou `DateTime64`. Paramètre facultatif.

Lors de la fusion, `ReplacingMergeTree` ne conserve qu&#39;une seule ligne parmi toutes celles ayant la même clé de tri :

* La dernière de la sélection, si `ver` n&#39;est pas défini. Une sélection est un ensemble de lignes dans un ensemble de parts participant à la fusion. La part créée le plus récemment (la dernière insertion) sera la dernière de la sélection. Ainsi, après déduplication, la toute dernière ligne de l&#39;insertion la plus récente sera conservée pour chaque clé de tri unique.
* Celle dont la version est maximale, si `ver` est spécifié. Si `ver` est identique pour plusieurs lignes, la règle &quot;si `ver` n&#39;est pas spécifié&quot; leur sera appliquée, c.-à-d. que la ligne insérée la plus récemment sera conservée.

Exemple :

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — Nom d’une colonne utilisée lors d’une fusion pour déterminer si les données de cette ligne représentent l’état ou si elles doivent être supprimées ; `1` correspond à une ligne &quot;supprimée&quot;, `0` à une ligne d’&quot;état&quot;.

Type de données de la colonne — `UInt8`.

:::note
`is_deleted` ne peut être activé que lorsque `ver` est utilisé.

Quelle que soit l’opération effectuée sur les données, la version doit être incrémentée. Si deux lignes insérées ont le même numéro de version, la dernière ligne insérée est conservée.

Par défaut, ClickHouse conserve la dernière ligne pour une clé, même s’il s’agit d’une ligne de suppression. Cela permet d’insérer en toute sécurité de futures lignes avec des versions inférieures, et la ligne de suppression sera toujours appliquée.

Pour supprimer définitivement ces lignes de suppression, activez le paramètre de table `allow_experimental_replacing_merge_with_cleanup`, puis effectuez l’une des opérations suivantes :

1. Définissez les paramètres de table `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`, `min_age_to_force_merge_on_partition_only` et `min_age_to_force_merge_seconds`. Si toutes les parts d’une partition sont plus anciennes que `min_age_to_force_merge_seconds`, ClickHouse les fusionnera
   toutes en une seule part et supprimera toutes les lignes de suppression.

2. Exécutez manuellement `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`.
   :::

Exemple :

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

<div id="query-clauses">
  ## Clauses de requête
</div>

Lors de la création d&#39;une table `ReplacingMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) sont requises que pour la création d&#39;une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète de création d&#39;une table</summary>

  :::note
  N&#39;utilisez pas cette méthode dans les nouveaux projets et, si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  Tous les paramètres, à l&#39;exception de `ver`, ont la même signification que dans `MergeTree`.

  * `ver` - colonne contenant la version. Paramètre facultatif. Pour une description, voir le texte ci-dessus.
</details>

<div id="query-time-de-duplication--final">
  ## Déduplication à l’exécution de la requête &amp; FINAL
</div>

Au moment de la fusion, ReplacingMergeTree identifie les lignes dupliquées en utilisant les valeurs des colonnes `ORDER BY` (utilisées pour créer la table) comme identifiant unique, et ne conserve que la version la plus élevée. Cela n’offre toutefois qu’une exactitude à terme : rien ne garantit que les lignes seront dédupliquées, et vous ne devez pas vous y fier. Les requêtes peuvent donc produire des résultats incorrects, car elles prennent en compte les lignes mises à jour et les lignes supprimées.

Pour obtenir des résultats corrects, les utilisateurs doivent compléter les fusions en arrière-plan par une déduplication à l’exécution de la requête et la suppression des lignes supprimées. Pour cela, vous pouvez utiliser l’opérateur `FINAL`. Par exemple, considérez l’exemple suivant :

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

Exécuter une requête sans `FINAL` donne un décompte incorrect (le résultat exact varie selon les fusions) :

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

L’ajout de final donne un résultat correct :

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Pour en savoir plus sur `FINAL`, notamment sur l’optimisation de ses performances, nous vous recommandons de consulter notre [guide détaillé sur ReplacingMergeTree](/fr/guides/replacing-merge-tree).