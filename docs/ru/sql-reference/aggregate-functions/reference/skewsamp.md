---
toc_priority: 151
---

# skewSamp {#skewsamp}

Вычисляет [выборочный коэффициент асимметрии](https://ru.wikipedia.org/wiki/Статистика_(функция_выборки)) для последовательности.

Он представляет собой несмещенную оценку асимметрии случайной величины, если переданные значения образуют ее выборку.

``` sql
skewSamp(expr)
```

**Параметры**

`expr` — [Выражение](../../syntax.md#syntax-expressions), возвращающее число.

**Возвращаемое значение**

Коэффициент асимметрии заданного распределения. Тип — [Float64](../../../sql-reference/data-types/float.md). Если `n <= 1` (`n` — размер выборки), тогда функция возвращает `nan`.

**Пример**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewsamp/) <!--hide-->
