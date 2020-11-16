---
machine_translated: true
machine_translated_rev: 71d72c1f237f4a553fe91ba6c6c633e81a49e35b
---

# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` veri türü, toplama işlevinin geçerli değerini depolar ve tam durumunu şu şekilde depolamaz [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) yapar. Bu optimizasyon, aşağıdaki özelliğin bulunduğu işlevlere uygulanabilir: bir işlev uygulama sonucu `f` bir satır kümesi için `S1 UNION ALL S2` uygulayarak elde edilebilir `f` satır parçalarına ayrı ayrı ayarlayın ve sonra tekrar uygulayın `f` sonuçlara: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. Bu özellik, kısmi toplama sonuçlarının Birleşik olanı hesaplamak için yeterli olduğunu garanti eder, bu nedenle herhangi bir ek veri depolamak ve işlemek zorunda kalmayız.

Aşağıdaki toplama işlevleri desteklenir:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)
-   [`groupArrayArray`](../../sql-reference/aggregate-functions/reference.md#agg_function-grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference.md#groupuniqarrayx-groupuniqarraymax-sizex)

Değerleri `SimpleAggregateFunction(func, Type)` bak ve aynı şekilde saklanır `Type`, bu yüzden fonksiyonları ile uygulamak gerekmez `-Merge`/`-State` sonekler. `SimpleAggregateFunction` daha iyi performans vardır `AggregateFunction` aynı toplama fonksiyonu ile.

**Parametre**

-   Toplama işlevinin adı.
-   Toplama işlevi bağımsız değişkenleri türleri.

**Örnek**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
