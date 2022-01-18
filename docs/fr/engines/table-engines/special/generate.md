---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

Le moteur de table GenerateRandom produit des données aléatoires pour un schéma de table donné.

Exemples d'utilisation:

-   Utiliser dans le test pour remplir une grande table reproductible.
-   Générer une entrée aléatoire pour les tests de fuzzing.

## Utilisation dans le serveur ClickHouse {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

Le `max_array_length` et `max_string_length` les paramètres spécifient la longueur maximale de tous
colonnes et chaînes de tableau en conséquence dans les données générées.

Générer le moteur de table prend en charge uniquement `SELECT` requête.

Il prend en charge tous les [Les types de données](../../../sql-reference/data-types/index.md) cela peut être stocké dans une table sauf `LowCardinality` et `AggregateFunction`.

**Exemple:**

**1.** Configurer le `generate_engine_table` table:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Interroger les données:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Les détails de mise en Œuvre {#details-of-implementation}

-   Pas pris en charge:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   Index
    -   Réplication

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
