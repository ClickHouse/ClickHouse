---
toc_priority: 145
---

# rankCorr {#agg_function-rankcorr}

Вычисляет коэффициент ранговой корреляции.

**Синтаксис**

``` sql
rankCorr(x, y)
```

**Аргументы**

-   `x` — произвольное значение. [Float32](../../../sql-reference/data-types/float.md#float32-float64) или [Float64](../../../sql-reference/data-types/float.md#float32-float64).
-   `y` — произвольное значение. [Float32](../../../sql-reference/data-types/float.md#float32-float64) или [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Возвращаемое значение**

-   Возвращает коэффициент ранговой корреляции рангов x и y. Значение коэффициента корреляции изменяется в пределах от -1 до +1. Если передается менее двух аргументов, функция возвращает исключение. Значение, близкое к +1, указывает на высокую линейную зависимость, и с увеличением одной случайной величины увеличивается и вторая случайная величина. Значение, близкое к -1, указывает на высокую линейную зависимость, и с увеличением одной случайной величины вторая случайная величина уменьшается. Значение, близкое или равное 0, означает отсутствие связи между двумя случайными величинами.

Тип: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Пример**

Запрос:

``` sql
SELECT rankCorr(number, number) FROM numbers(100);
```

Результат:

``` text
┌─rankCorr(number, number)─┐
│                        1 │
└──────────────────────────┘
```

Запрос:

``` sql
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);
```

Результат:

``` text
┌─roundBankers(rankCorr(exp(number), sin(number)), 3)─┐
│                                              -0.037 │
└─────────────────────────────────────────────────────┘
```
**Смотрите также**

-   [Коэффициент ранговой корреляции Спирмена](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D1%80%D1%80%D0%B5%D0%BB%D1%8F%D1%86%D0%B8%D1%8F#%D0%9A%D0%BE%D1%8D%D1%84%D1%84%D0%B8%D1%86%D0%B8%D0%B5%D0%BD%D1%82_%D1%80%D0%B0%D0%BD%D0%B3%D0%BE%D0%B2%D0%BE%D0%B9_%D0%BA%D0%BE%D1%80%D1%80%D0%B5%D0%BB%D1%8F%D1%86%D0%B8%D0%B8_%D0%A1%D0%BF%D0%B8%D1%80%D0%BC%D0%B5%D0%BD%D0%B0)