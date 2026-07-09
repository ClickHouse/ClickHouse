---
description: 'Documentation de la fonction arrayJoin'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'fonction arrayJoin'
doc_type: 'reference'
---

Il s&#39;agit d&#39;une fonction très inhabituelle.

Les fonctions ordinaires ne modifient pas un ensemble de lignes, mais seulement les valeurs de chaque ligne (`map`).
Les fonctions d&#39;agrégation condensent un ensemble de lignes (fold ou reduce).
La fonction `arrayJoin` prend chaque ligne et génère un ensemble de lignes (unfold).

Cette fonction prend un tableau comme argument et duplique la ligne source en plusieurs lignes, selon le nombre d&#39;éléments du tableau.
Toutes les valeurs des colonnes sont simplement copiées, à l&#39;exception de la valeur de la colonne à laquelle cette fonction est appliquée, qui est remplacée par la valeur correspondante du tableau.

:::note
Si le tableau est vide, `arrayJoin` ne produit aucune ligne.
Pour renvoyer une seule ligne contenant la valeur par défaut du type de tableau, vous pouvez l&#39;envelopper avec [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle), par exemple : `arrayJoin(emptyArrayToSingle(...))`.
:::

Par exemple :

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

La fonction `arrayJoin` s’applique à toutes les parties de la requête, y compris la clause `WHERE`. Notez que le résultat de la requête ci-dessous est `2`, même si la sous-requête n’a renvoyé qu’une seule ligne.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

Une requête peut utiliser plusieurs fonctions `arrayJoin`. Dans ce cas, la transformation est effectuée plusieurs fois et le nombre de lignes est multiplié.
Par exemple :

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### Bonne pratique
</div>

L’utilisation de plusieurs `arrayJoin` sur une même expression peut ne pas produire les résultats attendus en raison de l’élimination des sous-expressions communes.
Dans ce cas, pensez à modifier les expressions de tableau répétées en y ajoutant des opérations supplémentaires qui n’affectent pas le résultat de la jointure. Par exemple, `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

Exemple :

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

Notez la syntaxe [`ARRAY JOIN`](../statements/select/array-join.md) dans la requête SELECT, qui offre davantage de possibilités.
`ARRAY JOIN` permet de déplier simultanément plusieurs tableaux comportant le même nombre d’éléments.

Exemple :

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Vous pouvez aussi utiliser [`Tuple`](../data-types/tuple.md)

Exemple :

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Le nom `arrayJoin` dans ClickHouse vient de sa similarité conceptuelle avec l’opération JOIN, mais appliquée à des tableaux au sein d’une même ligne. Alors que les JOIN traditionnels combinent des lignes de tables différentes, `arrayJoin` « joint » chaque élément d’un tableau dans une ligne, produisant plusieurs lignes - une pour chaque élément du tableau - tout en dupliquant les valeurs des autres colonnes. ClickHouse fournit également la syntaxe de clause [`ARRAY JOIN`](/fr/sql-reference/statements/select/array-join), qui rend cette relation avec les opérations JOIN traditionnelles encore plus explicite en utilisant une terminologie SQL JOIN familière. Ce processus est aussi appelé « dépliage » du tableau, mais le terme « join » est utilisé à la fois dans le nom de la fonction et dans la clause, car il s’apparente à une jointure entre la table et les éléments du tableau, ce qui étend effectivement le jeu de données d’une manière similaire à une opération JOIN.