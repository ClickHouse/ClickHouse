---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: Array(T)
---

# Array(t) {#data-type-array}

Un tableau de `T`les éléments de type. `T` peut être n'importe quel type de données, y compris un tableau.

## La création d'un Tableau {#creating-an-array}

Vous pouvez utiliser une fonction pour créer un tableau:

``` sql
array(T)
```

Vous pouvez également utiliser des crochets.

``` sql
[]
```

Exemple de création d'un tableau:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## Utilisation de Types de données {#working-with-data-types}

Lors de la création d'un tableau à la volée, ClickHouse définit automatiquement le type d'argument comme le type de données le plus étroit pouvant stocker tous les arguments listés. S'il y a des [Nullable](nullable.md#data_type-nullable) ou littéral [NULL](../../sql-reference/syntax.md#null-literal) les valeurs, le type d'un élément de tableau devient également [Nullable](nullable.md).

Si ClickHouse n'a pas pu déterminer le type de données, il génère une exception. Par exemple, cela se produit lorsque vous essayez de créer un tableau avec des chaînes et des nombres simultanément (`SELECT array(1, 'a')`).

Exemples de détection automatique de type de données:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Si vous essayez de créer un tableau de types de données incompatibles, ClickHouse lève une exception:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

[Article Original](https://clickhouse.tech/docs/en/data_types/array/) <!--hide-->
