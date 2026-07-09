---
description: 'Documentation du type de données SimpleAggregateFunction'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'Type de données SimpleAggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Description
</div>

Le type de données `SimpleAggregateFunction` stocke l’état intermédiaire d’une
fonction d’agrégation, mais pas son état complet, contrairement au type
[`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md).

Cette optimisation peut être appliquée aux fonctions pour lesquelles la propriété
suivante est vérifiée :

> le résultat de l’application d’une fonction `f` à un ensemble de lignes `S1 UNION ALL S2` peut
> être obtenu en appliquant `f` séparément à des sous-ensembles de lignes, puis en
> appliquant à nouveau `f` aux résultats : `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

Cette propriété garantit que les résultats d’agrégation partielle suffisent pour calculer
le résultat combiné, sans qu’il soit nécessaire de stocker ni de traiter de données supplémentaires. Par
exemple, le résultat des fonctions `min` ou `max` ne nécessite aucune étape supplémentaire pour
obtenir le résultat final à partir des étapes intermédiaires, tandis que la fonction `avg`
nécessite de conserver une somme et un compte, qui seront divisés pour obtenir la
moyenne lors d’une étape finale `Merge` combinant les états intermédiaires.

Les valeurs de fonctions d’agrégation sont généralement produites en appelant une fonction d’agrégation
avec le combinateur [`-SimpleState`](/fr/sql-reference/aggregate-functions/combinators#-simplestate) ajouté au nom de la fonction.

<div id="syntax">
  ## Syntaxe
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Paramètres**

* `aggregate_function_name` - Le nom d’une fonction d’agrégation.
* `Type` - Types des arguments de la fonction d’agrégation.

<div id="supported-functions">
  ## Fonctions prises en charge
</div>

Les fonctions d&#39;agrégation suivantes sont prises en charge :

* [`any`](/fr/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/fr/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/fr/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/fr/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/fr/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/fr/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/fr/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/fr/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/fr/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/fr/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/fr/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/fr/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/fr/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/fr/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/fr/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
Les valeurs de `SimpleAggregateFunction(func, Type)` sont du même `Type`.
Contrairement au type `AggregateFunction`, il n&#39;est donc pas nécessaire d&#39;appliquer les combinateurs `-Merge`/`-State`.

Le type `SimpleAggregateFunction` offre de meilleures performances que le type `AggregateFunction`
pour les mêmes fonctions d&#39;agrégation.
:::

<div id="example">
  ## Exemple
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Utiliser les combinateurs d’agrégation dans ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - Blog : [Utiliser les combinateurs d’agrégation dans ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* Type [AggregateFunction](/fr/sql-reference/data-types/aggregatefunction).