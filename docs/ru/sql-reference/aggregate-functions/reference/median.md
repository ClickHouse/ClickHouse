# median {#median}

Функции `median*` — синонимы для соответствущих функций `quantile*`. Они вычисляют медиану числовой последовательности.

Функции:


-   `median` — синоним для [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile).
-   `medianDeterministic` — синоним для [quantileDeterministic](../../../sql-reference/aggregate-functions/reference/quantiledeterministic.md#quantiledeterministic).
-   `medianExact` — синоним для [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact).
-   `medianExactWeighted` — синоним для [quantileExactWeighted](../../../sql-reference/aggregate-functions/reference/quantileexactweighted.md#quantileexactweighted).
-   `medianTiming` — синоним для [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming).
-   `medianTimingWeighted` — синоним для [quantileTimingWeighted](../../../sql-reference/aggregate-functions/reference/quantiletimingweighted.md#quantiletimingweighted).
-   `medianTDigest` — синоним для [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md#quantiletdigest).
-   `medianTDigestWeighted` — синоним для [quantileTDigestWeighted](../../../sql-reference/aggregate-functions/reference/quantiletdigestweighted.md#quantiletdigestweighted).
-   `medianBFloat16` — синоним для [quantileBFloat16](../../../sql-reference/aggregate-functions/reference/quantilebfloat16.md#quantilebfloat16).

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
SELECT medianDeterministic(val, 1) FROM t
```

Результат:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

