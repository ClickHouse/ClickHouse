---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: AggregateFunction (nom, types_of_arguments...)
---

# AggregateFunction(name, types_of_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [une vue matérialisée](../../sql-reference/statements/create.md#create-view). La manière courante de produire un État de fonction d'agrégat est d'appeler la fonction d'agrégat avec le `-State` suffixe. Pour obtenir le résultat final de l'agrégation dans l'avenir, vous devez utiliser la même fonction d'agrégation avec la `-Merge`suffixe.

`AggregateFunction` — parametric data type.

**Paramètre**

-   Nom de la fonction d'agrégation.

        If the function is parametric, specify its parameters too.

-   Types des arguments de la fonction d'agrégation.

**Exemple**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq), anyIf ([tout](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[Si](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) et [les quantiles](../../sql-reference/aggregate-functions/reference.md) les fonctions d'agrégation sont-elles prises en charge dans ClickHouse.

## Utilisation {#usage}

### Insertion De Données {#data-insertion}

Pour insérer des données, utilisez `INSERT SELECT` avec le regroupement d' `-State`- fonction.

**Exemples de fonction**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

Contrairement aux fonctions correspondantes `uniq` et `quantiles`, `-State`- les fonctions renvoient l'état, au lieu de la valeur finale. En d'autres termes, ils renvoient une valeur de `AggregateFunction` type.

Dans les résultats de `SELECT` requête, les valeurs de `AggregateFunction` type ont une représentation binaire spécifique à l'implémentation pour tous les formats de sortie ClickHouse. Si les données de vidage dans, par exemple, `TabSeparated` format avec `SELECT` requête, puis ce vidage peut être chargé en utilisant `INSERT` requête.

### Sélection De Données {#data-selection}

Lors de la sélection des données `AggregatingMergeTree` table, utilisez `GROUP BY` et les mêmes fonctions d'agrégat que lors de l'insertion de données, mais en utilisant `-Merge`suffixe.

Une fonction d'agrégation avec `-Merge` suffixe prend un ensemble d'états, les combine, et renvoie le résultat complet de l'agrégation de données.

Par exemple, les deux requêtes suivantes retournent le même résultat:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Exemple D'Utilisation {#usage-example}

Voir [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) Description du moteur.

[Article Original](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
