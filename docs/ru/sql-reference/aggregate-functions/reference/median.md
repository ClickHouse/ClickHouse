# median {#median}

Функции `median*` — алиасы для соответствущих функций `quantile*`. Они вычисляют медиану числовой последовательности.

Functions:

-   `median` — алиас [quantile](#quantile).
-   `medianDeterministic` — алиас [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — алиас [quantileExact](#quantileexact).
-   `medianExactWeighted` — алиас [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — алиас [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — алиас [quantileTimingWeighted](#quantiletimingweighted).
-   `medianTDigest` — алиас [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — алиас [quantileTDigestWeighted](#quantiletdigestweighted).

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

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/median/) <!--hide-->
