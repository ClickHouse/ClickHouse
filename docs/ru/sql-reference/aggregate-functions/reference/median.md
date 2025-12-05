---
slug: /ru/sql-reference/aggregate-functions/reference/median
---
# median {#median}

Функции `median*` — синонимы для соответствущих функций `quantile*`. Они вычисляют медиану числовой последовательности.

Функции:

-   `median` — синоним для [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile).
-   `medianDeterministic` — синоним для [quantileDeterministic](/sql-reference/aggregate-functions/reference/quantiledeterministic).
-   `medianExact` — синоним для [quantileExact](/sql-reference/aggregate-functions/reference/quantileexact#quantileexact).
-   `medianExactWeighted` — синоним для [quantileExactWeighted](/sql-reference/aggregate-functions/reference/quantileexactweighted).
-   `medianTiming` — синоним для [quantileTiming](/sql-reference/aggregate-functions/reference/quantiletiming).
-   `medianTimingWeighted` — синоним для [quantileTimingWeighted](/sql-reference/aggregate-functions/reference/quantiletimingweighted).
-   `medianTDigest` — синоним для [quantileTDigest](/sql-reference/aggregate-functions/reference/quantiletdigest).
-   `medianTDigestWeighted` — синоним для [quantileTDigestWeighted](/sql-reference/aggregate-functions/reference/quantiletdigestweighted).
-   `medianBFloat16` — синоним для [quantileBFloat16](/sql-reference/aggregate-functions/reference/quantilebfloat16).

**Пример**

Входная таблица:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Запрос:

``` sql
SELECT medianDeterministic(val, 1) FROM t;
```

Результат:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```
