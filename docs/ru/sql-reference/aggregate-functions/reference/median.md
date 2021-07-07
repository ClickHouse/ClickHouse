# median {#median}

Функции `median*` — алиасы для соответствущих функций `quantile*`. Они вычисляют медиану числовой последовательности.

Функции:

-   `median` — синоним [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile).
-   `medianDeterministic` — синоним [quantileDeterministic](../../../sql-reference/aggregate-functions/reference/quantiledeterministic.md#quantiledeterministic).
-   `medianExact` — синоним [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact).
-   `medianExactWeighted` — синоним [quantileExactWeighted](../../../sql-reference/aggregate-functions/reference/quantileexactweighted.md#quantileexactweighted).
-   `medianTiming` — синоним [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming).
-   `medianTimingWeighted` — синоним [quantileTimingWeighted](../../../sql-reference/aggregate-functions/reference/quantiletimingweighted.md#quantiletimingweighted).
-   `medianTDigest` — синоним [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md#quantiletdigest).
-   `medianTDigestWeighted` — синоним [quantileTDigestWeighted](../../../sql-reference/aggregate-functions/reference/quantiletdigestweighted.md#quantiletdigestweighted).

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
