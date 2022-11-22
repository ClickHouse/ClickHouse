---
toc_priority: 209
---

# quantileBFloat16 {#quantilebfloat16}

Приближенно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) выборки чисел в формате [bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format). `bfloat16` — это формат с плавающей точкой, в котором для представления числа используется 1 знаковый бит, 8 бит для порядка и 7 бит для мантиссы.
Функция преобразует входное число в 32-битное с плавающей точкой и обрабатывает его старшие 16 бит. Она вычисляет квантиль в формате `bfloat16` и преобразует его в 64-битное число с плавающей точкой, добавляя нулевые биты.
Эта функция выполняет быстрые приближенные вычисления с относительной ошибкой не более 0.390625%.

**Синтаксис**

``` sql
quantileBFloat16[(level)](expr)
```

Синоним: `medianBFloat16`

**Аргументы**

-   `expr` — столбец с числовыми данными. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md).

**Параметры**

-   `level` — уровень квантиля. Необязательный параметр. Допустимый диапазон значений от 0 до 1. Значение по умолчанию: 0.5. [Float](../../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Приближенное значение квантиля.

Тип: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Пример**

В таблице есть столбцы с целыми числами и с числами с плавающей точкой:

``` text
┌─a─┬─────b─┐
│ 1 │ 1.001 │
│ 2 │ 1.002 │
│ 3 │ 1.003 │
│ 4 │ 1.004 │
└───┴───────┘
```

Запрос для вычисления 0.75-квантиля (верхнего квартиля):

``` sql
SELECT quantileBFloat16(0.75)(a), quantileBFloat16(0.75)(b) FROM example_table;
```

Результат:

``` text
┌─quantileBFloat16(0.75)(a)─┬─quantileBFloat16(0.75)(b)─┐
│                         3 │                         1 │
└───────────────────────────┴───────────────────────────┘
```
Обратите внимание, что все числа с плавающей точкой в примере были округлены до 1.0 при преобразовании к `bfloat16`.

# quantileBFloat16Weighted {#quantilebfloat16weighted}

Версия функции `quantileBFloat16`, которая учитывает вес каждого элемента последовательности.

**См. также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
