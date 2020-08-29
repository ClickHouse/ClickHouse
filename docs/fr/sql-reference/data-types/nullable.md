---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Nullable
---

# Nullable(typename) {#data_type-nullable}

Permet de stocker marqueur spécial ([NULL](../../sql-reference/syntax.md)) qui dénote “missing value” aux valeurs normales autorisées par `TypeName`. Par exemple, un `Nullable(Int8)` type colonne peut stocker `Int8` type de valeurs, et les lignes qui n'ont pas de valeur magasin `NULL`.

Pour un `TypeName` vous ne pouvez pas utiliser les types de données composites [Tableau](array.md) et [Tuple](tuple.md). Les types de données composites peuvent contenir `Nullable` valeurs de type, telles que `Array(Nullable(Int8))`.

A `Nullable` le champ type ne peut pas être inclus dans les index de table.

`NULL` est la valeur par défaut pour tout `Nullable` type, sauf indication contraire dans la configuration du serveur ClickHouse.

## Caractéristiques De Stockage {#storage-features}

Stocker `Nullable` valeurs de type dans une colonne de table, ClickHouse utilise un fichier séparé avec `NULL` masques en plus du fichier normal avec des valeurs. Les entrées du fichier masks permettent à ClickHouse de faire la distinction entre `NULL` et une valeur par défaut du type de données correspondant pour chaque ligne de table. En raison d'un fichier supplémentaire, `Nullable` colonne consomme de l'espace de stockage supplémentaire par rapport à une normale similaire.

!!! info "Note"
    Utiliser `Nullable` affecte presque toujours négativement les performances, gardez cela à l'esprit lors de la conception de vos bases de données.

## Exemple D'Utilisation {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
