---
description: 'CoalescingMergeTree hérite du moteur MergeTree. Sa principale caractéristique
  est sa capacité à stocker automatiquement la dernière valeur non nulle de chaque colonne lors des fusions de part de données.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'Moteur de table CoalescingMergeTree'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note Disponible à partir de la version 25.6
Ce moteur de table est disponible à partir de la version 25.6 et dans les versions ultérieures, aussi bien en OSS que dans Cloud.
:::

Ce moteur hérite de [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree). La principale différence réside dans la manière dont les part de données sont fusionnées : pour les tables `CoalescingMergeTree`, ClickHouse remplace toutes les lignes ayant la même clé primaire (ou, plus précisément, la même [clé de tri](../../../engines/table-engines/mergetree-family/mergetree.md)) par une seule ligne contenant la dernière valeur non NULL de chaque colonne.

Cela permet des upserts au niveau des colonnes, ce qui signifie que vous pouvez mettre à jour uniquement certaines colonnes plutôt que des lignes entières.

`CoalescingMergeTree` est conçu pour être utilisé avec des types Nullable dans les colonnes qui ne font pas partie de la clé. Si les colonnes ne sont pas Nullable, le comportement est le même qu&#39;avec [ReplacingMergeTree](/fr/engines/table-engines/mergetree-family/replacingmergetree).

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de la requête, consultez la [description de la requête](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-coalescingmergetree">
  ### Paramètres de CoalescingMergeTree
</div>

<div id="columns">
  #### Colonnes
</div>

`columns` - Facultatif. Un tuple contenant les noms des colonnes dont les valeurs seront fusionnées. Les colonnes indiquées ne doivent pas faire partie de la partition ni de la clé de tri. Si `columns` n&#39;est pas spécifié, ClickHouse fusionne les valeurs de toutes les colonnes qui ne font pas partie de la clé de tri.

<div id="query-clauses">
  ### Clauses de requête
</div>

Lors de la création d’une table `CoalescingMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) sont requises que pour la création d’une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète de création d’une table</summary>

  :::note
  N’utilisez pas cette méthode dans de nouveaux projets et, si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Tous les paramètres, à l’exception de `columns`, ont la même signification que dans `MergeTree`.

  * `columns` — tuple contenant les noms des colonnes dont les valeurs seront additionnées. Paramètre facultatif. Pour plus de détails, voir le texte ci-dessus.
</details>

<div id="usage-example">
  ## Exemple d’utilisation
</div>

Considérons la table suivante :

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

Insérez-y des données :

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

Le résultat ressemblera à ceci :

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

Requête recommandée pour un résultat correct et définitif :

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

L’utilisation du modificateur `FINAL` force ClickHouse à appliquer la logique de fusion au moment de la requête, ce qui garantit d’obtenir pour chaque colonne la valeur « la plus récente » correcte et consolidée. C’est la méthode la plus sûre et la plus précise pour interroger une table CoalescingMergeTree.

:::note

Une approche avec `GROUP BY` peut renvoyer des résultats incorrects si les parts sous-jacentes n’ont pas encore été entièrement fusionnées.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Agrégation des éléments de Tuple
</div>

Lorsque le paramètre `allow_tuple_element_aggregation` est activé, les colonnes `Tuple` sont aplaties récursivement afin que chaque élément terminal participe indépendamment à la coalescence. Cela vous permet de stocker plusieurs champs dans une seule colonne `Tuple` et de les faire coalescer élément par élément lors des fusions — chaque sous-colonne `Nullable` conserve indépendamment la dernière valeur non-NULL.

Les mêmes règles s&#39;appliquent aux sous-colonnes aplaties qu&#39;aux colonnes ordinaires :

* Les sous-colonnes appartenant à un `Tuple` dans la clé de tri ou la clé de partitionnement sont exclues de la coalescence.
* Si `columns` est spécifié, seules les sous-colonnes des colonnes `Tuple` répertoriées sont coalescées.

:::note
Ce paramètre est immuable et doit être spécifié lors de la création de la table.
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```