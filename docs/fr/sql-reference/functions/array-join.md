---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: arrayJoin
---

# fonction arrayJoin {#functions_arrayjoin}

C'est un très inhabituelle de la fonction.

Les fonctions normales ne modifient pas un ensemble de lignes, mais modifient simplement les valeurs de chaque ligne (map).
Les fonctions d'agrégation compriment un ensemble de lignes (plier ou réduire).
Le ‘arrayJoin’ la fonction prend chaque ligne et génère un ensemble de lignes (dépliante).

Cette fonction prend un tableau comme argument et propage la ligne source à plusieurs lignes pour le nombre d'éléments dans le tableau.
Toutes les valeurs des colonnes sont simplement copiés, sauf les valeurs dans la colonne où cette fonction est appliquée; elle est remplacée par la valeur correspondante de tableau.

Une requête peut utiliser plusieurs `arrayJoin` fonction. Dans ce cas, la transformation est effectuée plusieurs fois.

Notez la syntaxe de jointure de tableau dans la requête SELECT, qui offre des possibilités plus larges.

Exemple:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

``` text
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/array_join/) <!--hide-->
