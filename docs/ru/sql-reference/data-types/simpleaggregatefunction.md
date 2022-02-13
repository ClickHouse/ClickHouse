# SimpleAggregateFunction(func, type) {#data-type-simpleaggregatefunction}

Хранит только текущее значение агрегатной функции и не сохраняет ее полное состояние, как это делает [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md). Такая оптимизация может быть применена к функциям, которые обладают следующим свойством: результат выполнения функции `f` к набору строк `S1 UNION ALL S2` может быть получен путем выполнения `f` к отдельным частям набора строк,
а затем повторного выполнения `f` к результатам: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. Это свойство гарантирует, что результатов частичной агрегации достаточно для вычисления комбинированной, поэтому хранить и обрабатывать какие-либо дополнительные данные не требуется.

Чтобы получить промежуточное значение, обычно используются агрегатные функции с суффиксом [-SimpleState](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-simplestate).

Поддерживаются следующие агрегатные функции:

-   [`any`](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference/anylast.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum)
-   [`sumWithOverflow`](../../sql-reference/aggregate-functions/reference/sumwithoverflow.md#sumwithoverflowx)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference/groupbitand.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference/groupbitor.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference/groupbitxor.md#groupbitxor)
-   [`groupArrayArray`](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)
-   [`sumMap`](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap)
-   [`minMap`](../../sql-reference/aggregate-functions/reference/minmap.md#agg_functions-minmap)
-   [`maxMap`](../../sql-reference/aggregate-functions/reference/maxmap.md#agg_functions-maxmap)

!!! note "Примечание"
    Значения `SimpleAggregateFunction(func, Type)` отображаются и хранятся так же, как и `Type`, поэтому комбинаторы [-Merge](../../sql-reference/aggregate-functions/combinators.md#aggregate_functions_combinators-merge) и [-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state) не требуются.

    `SimpleAggregateFunction` имеет лучшую производительность, чем `AggregateFunction` с той же агрегатной функцией.

**Параметры**

-   `func` — имя агрегатной функции.
-   `type` — типы аргументов агрегатной функции.

**Пример**

``` sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

