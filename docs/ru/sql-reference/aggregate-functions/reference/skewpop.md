---
toc_priority: 150
---

# skewPop {#skewpop}

Вычисляет [коэффициент асимметрии](https://ru.wikipedia.org/wiki/Коэффициент_асимметрии) для последовательности.

``` sql
skewPop(expr)
```

**Параметры**

`expr` — [Выражение](../../syntax.md#syntax-expressions), возвращающее число.

**Возвращаемое значение**

Коэффициент асимметрии заданного распределения. Тип — [Float64](../../../sql-reference/data-types/float.md)

**Пример**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewpop/) <!--hide-->
