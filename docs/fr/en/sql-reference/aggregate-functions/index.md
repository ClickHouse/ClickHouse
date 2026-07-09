---
description: 'Documentation des fonctions d''agrégation'
sidebar_label: 'Fonctions d''agrégation'
sidebar_position: 33
slug: /sql-reference/aggregate-functions/
title: 'Fonctions d''agrégation'
doc_type: 'reference'
---

Les fonctions d&#39;agrégation fonctionnent de manière [ordinaire](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial), comme les experts en bases de données s&#39;y attendent.

ClickHouse prend également en charge :

* Les [fonctions d&#39;agrégation paramétriques](/fr/sql-reference/aggregate-functions/parametric-functions), qui acceptent d&#39;autres paramètres en plus des colonnes.
* Les [combinateurs](/fr/sql-reference/aggregate-functions/combinators), qui modifient le comportement des fonctions d&#39;agrégation.

<div id="null-processing">
  ## Traitement des `NULL`
</div>

Lors de l’agrégation, tous les arguments `NULL` sont ignorés. Si l’agrégation comporte plusieurs arguments, elle ignore toute ligne dans laquelle l’un d’eux au moins est `NULL`.

Il existe une exception à cette règle : les fonctions [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md), [`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) et leurs alias (`any` et `anyLast`, respectivement), lorsqu’elles sont suivies du modificateur `RESPECT NULLS`. Par exemple, `FIRST_VALUE(b) RESPECT NULLS`.

**Exemples :**

Prenons cette table :

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Supposons que vous souhaitiez faire la somme des valeurs de la colonne `y` :

```sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

Vous pouvez maintenant utiliser la fonction `groupArray` pour créer un tableau à partir de la colonne `y` :

```sql
SELECT groupArray(y) FROM t_null_big
```

```text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` n’inclut pas `NULL` dans le tableau résultant.

Vous pouvez utiliser [COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) pour remplacer `NULL` par une valeur adaptée à votre cas d’utilisation. Par exemple : `avg(COALESCE(column, 0))` utilisera la valeur de la colonne dans l’agrégation, ou zéro si `NULL` :

```sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

```text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

Vous pouvez également utiliser [Tuple](/fr/sql-reference/data-types/tuple.md) pour contourner le comportement qui consiste à ignorer les valeurs `NULL`. Un `Tuple` qui contient uniquement une valeur `NULL` n’est pas `NULL` ; les fonctions d’agrégation n’ignoreront donc pas cette ligne à cause de cette valeur `NULL`.

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

Notez que les agrégations ne sont pas prises en compte lorsque les colonnes sont utilisées comme arguments d’une fonction d’agrégation.  Par exemple, [`count`](../../sql-reference/aggregate-functions/reference/count.md) sans paramètres (`count()`) ou avec des valeurs constantes (`count(1)`) comptera toutes les lignes du bloc (indépendamment de la valeur de la colonne du GROUP BY, puisqu’elle n’est pas un argument), tandis que `count(column)` ne renverra que le nombre de lignes pour lesquelles column n’est pas NULL.

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

Et voici un exemple de first&#95;value avec `RESPECT NULLS`, où l&#39;on peut voir que les valeurs NULL en entrée sont respectées et que la fonction renvoie la première valeur lue, qu&#39;elle soit NULL ou non :

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) AS range,
    first_value(odd_or_null) AS first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) AS odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```