---
toc_priority: 153
---

# kurtPop {#kurtpop}

Вычисляет [коэффициент эксцесса](https://ru.wikipedia.org/wiki/Коэффициент_эксцесса) последовательности.

``` sql
kurtPop(expr)
```

**Параметры**

`expr` — [Выражение](../../syntax.md#syntax-expressions), возвращающее число.

**Возвращаемое значение**

Коэффициент эксцесса заданного распределения. Тип — [Float64](../../../sql-reference/data-types/float.md)

**Пример**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/kurtpop/) <!--hide-->
