---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` le type de données stocke la valeur actuelle de la fonction d'agrégat et ne stocke pas son état complet comme [`AggregateFunction`](aggregatefunction.md) faire. Cette optimisation peut être appliquée aux fonctions pour lesquelles la propriété suivante est conservée: le résultat de l'application d'une fonction `f` pour un ensemble de lignes `S1 UNION ALL S2` peut être obtenu en appliquant `f` pour les parties de la ligne définie séparément, puis à nouveau l'application `f` pour les résultats: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. Cette propriété garantit que les résultats d'agrégation partielle sont suffisants pour calculer le combiné, de sorte que nous n'avons pas à stocker et traiter de données supplémentaires.

Les fonctions d'agrégation suivantes sont prises en charge:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)

Les valeurs de la `SimpleAggregateFunction(func, Type)` regarder et stockées de la même manière que `Type`, de sorte que vous n'avez pas besoin d'appliquer des fonctions avec `-Merge`/`-State` suffixe. `SimpleAggregateFunction` a de meilleures performances que `AggregateFunction` avec la même fonction d'agrégation.

**Paramètre**

-   Nom de la fonction d'agrégation.
-   Types des arguments de la fonction d'agrégation.

**Exemple**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[Article Original](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
