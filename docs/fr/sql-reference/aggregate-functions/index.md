---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Les Fonctions D'Agr\xE9gation"
toc_priority: 33
toc_title: Introduction
---

# Les Fonctions D'Agrégation {#aggregate-functions}

Les fonctions d'agrégation fonctionnent dans le [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) comme prévu par les experts de la base de données.

Clickhouse prend également en charge:

-   [Fonctions d'agrégat paramétriques](parametric-functions.md#aggregate_functions_parametric) qui acceptent d'autres paramètres en plus des colonnes.
-   [Combinators](combinators.md#aggregate_functions_combinators), qui modifient le comportement des fonctions d'agrégation.

## Le Traitement NULL {#null-processing}

Au cours de l'agrégation, tous les `NULL`s sont ignorés.

**Exemple:**

Considérez ce tableau:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Disons que vous devez totaliser les valeurs dans le `y` colonne:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

Le `sum` la fonction d'interprète `NULL` comme `0`. En particulier, cela signifie que si la fonction reçoit en entrée d'une sélection où toutes les valeurs sont `NULL`, alors le résultat sera `0`, pas `NULL`.

Maintenant, vous pouvez utiliser le `groupArray` fonction pour créer un tableau à partir `y` colonne:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` ne comprend pas `NULL` dans le tableau résultant.

[Article Original](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
