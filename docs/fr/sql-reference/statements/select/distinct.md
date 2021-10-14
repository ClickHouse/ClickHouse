---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# La Clause DISTINCT {#select-distinct}

Si `SELECT DISTINCT` est spécifié, seules les lignes uniques restera un résultat de requête. Ainsi, une seule ligne restera hors de tous les ensembles de lignes entièrement correspondantes dans le résultat.

## Le Traitement Null {#null-processing}

`DISTINCT` fonctionne avec [NULL](../../syntax.md#null-literal) comme si `NULL` ont une valeur spécifique, et `NULL==NULL`. En d'autres termes, dans le `DISTINCT` résultats, différentes combinaisons avec `NULL` une fois seulement. Elle diffère de `NULL` traitement dans la plupart des autres contextes.

## Alternative {#alternatives}

Il est possible d'obtenir le même résultat en appliquant [GROUP BY](group-by.md) sur le même ensemble de valeurs, comme spécifié comme `SELECT` clause, sans utiliser de fonctions d'agrégation. Mais il y a peu de différences de `GROUP BY` approche:

-   `DISTINCT` peut être utilisé avec d' `GROUP BY`.
-   Lorsque [ORDER BY](order-by.md) est omis et [LIMIT](limit.md) est définie, la requête s'arrête immédiatement après le nombre de lignes différentes, a été lu.
-   Les blocs de données sont produits au fur et à mesure qu'ils sont traités, sans attendre que la requête entière se termine.

## Limitation {#limitations}

`DISTINCT` n'est pas pris en charge si `SELECT` a au moins une colonne de tableau.

## Exemple {#examples}

Clickhouse prend en charge l'utilisation du `DISTINCT` et `ORDER BY` clauses pour différentes colonnes dans une requête. Le `DISTINCT` la clause est exécutée avant la `ORDER BY` clause.

Exemple de table:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Lors de la sélection de données avec le `SELECT DISTINCT a FROM t1 ORDER BY b ASC` requête, nous obtenons le résultat suivant:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Si nous changeons la direction de tri `SELECT DISTINCT a FROM t1 ORDER BY b DESC`, nous obtenons le résultat suivant:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Rangée `2, 4` a été coupé avant de les trier.

Prenez en compte cette spécificité d'implémentation lors de la programmation des requêtes.
