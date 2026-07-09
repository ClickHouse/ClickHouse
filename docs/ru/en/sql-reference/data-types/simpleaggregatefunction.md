---
description: 'Документация для типа данных SimpleAggregateFunction'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'Тип данных SimpleAggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Описание
</div>

Тип данных `SimpleAggregateFunction` хранит промежуточное состояние
агрегатной функции, но не её полное состояние, в отличие от типа
[`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md).

Эту оптимизацию можно применять к функциям, для которых выполняется
следующее свойство:

> результат применения функции `f` к набору строк `S1 UNION ALL S2` можно
> получить, если отдельно применить `f` к частям набора строк, а затем ещё раз
> применить `f` к полученным результатам: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

Это свойство гарантирует, что частичных результатов агрегации достаточно для вычисления
общего результата, поэтому не нужно хранить и обрабатывать какие-либо дополнительные данные. Например,
для функций `min` или `max` не нужны дополнительные шаги, чтобы получить
итоговый результат из промежуточных, тогда как для функции `avg`
нужно хранить сумму и количество, которые затем делятся для получения
среднего значения на финальном шаге `Merge`, объединяющем промежуточные состояния.

Значения агрегатных функций обычно получают, вызывая агрегатную функцию
с комбинатором [`-SimpleState`](/ru/sql-reference/aggregate-functions/combinators#-simplestate), добавленным к имени функции.

<div id="syntax">
  ## Синтаксис
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Параметры**

* `aggregate_function_name` - Название агрегатной функции.
* `Type` - Типы аргументов агрегатной функции.

<div id="supported-functions">
  ## Поддерживаемые функции
</div>

Поддерживаются следующие агрегатные функции:

* [`any`](/ru/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/ru/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/ru/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/ru/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/ru/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/ru/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/ru/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/ru/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/ru/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/ru/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/ru/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/ru/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/ru/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/ru/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/ru/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
Значения `SimpleAggregateFunction(func, Type)` имеют тот же `Type`,
поэтому, в отличие от типа `AggregateFunction`,
к ним не нужно применять комбинаторы `-Merge`/`-State`.

Тип `SimpleAggregateFunction` обеспечивает более высокую производительность, чем `AggregateFunction`,
для тех же агрегатных функций.
:::

<div id="example">
  ## Пример
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## Материалы по теме
</div>

* Блог: [Использование комбинаторов агрегатных функций в ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - Блог: [Использование комбинаторов агрегатных функций в ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* Тип данных [AggregateFunction](/ru/sql-reference/data-types/aggregatefunction).