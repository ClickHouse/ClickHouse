---
description: 'Le moteur de table GenerateRandom produit des données aléatoires pour un
  schéma de table donné.'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'Moteur de table GenerateRandom'
doc_type: 'reference'
---

Le moteur de table GenerateRandom produit des données aléatoires pour un schéma de table donné.

Exemples d’utilisation :

* L’utiliser dans des tests pour remplir une grande table reproductible.
* Générer des données d’entrée aléatoires pour des tests de fuzzing.

<div id="usage-in-clickhouse-server">
  ## Utilisation dans ClickHouse Server
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

Les paramètres `max_array_length` et `max_string_length` définissent la longueur maximale, respectivement, de toutes les colonnes de type array ou map et des chaînes dans les données générées.

Le moteur de table Generate prend uniquement en charge les requêtes `SELECT`.

Il prend en charge tous les [types de données](../../../sql-reference/data-types/index.md) pouvant être stockés dans une table, à l&#39;exception de `AggregateFunction`.

<div id="example">
  ## Exemple
</div>

**1.** Configurez la table `generate_engine_table` :

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Interrogez les données :

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## Détails de la mise en œuvre
</div>

* Non pris en charge :
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * Index
  * Réplication