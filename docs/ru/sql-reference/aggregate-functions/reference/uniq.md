---
toc_priority: 190
---

# uniq {#agg_function-uniq}

Приближённо вычисляет количество различных значений аргумента.

``` sql
uniq(x[, ...])
```

**Аргументы**

Функция принимает переменное число входных параметров. Параметры могут быть числовых типов, а также `Tuple`, `Array`, `Date`, `DateTime`, `String`.

**Возвращаемое значение**

-   Значение с типом данных [UInt64](../../../sql-reference/data-types/int-uint.md).

**Детали реализации**

Функция:

-   Вычисляет хэш для всех параметров агрегации, а затем использует его в вычислениях.

-   Использует адаптивный алгоритм выборки. В качестве состояния вычисления функция использует выборку хэш-значений элементов размером до 65536.

        Этот алгоритм очень точен и очень эффективен по использованию CPU. Если запрос содержит небольшое количество этих функций, использование `uniq` почти так же эффективно,  как и использование других агрегатных функций.

-   Результат детерминирован (не зависит от порядка выполнения запроса).

Эту функцию рекомендуется использовать практически во всех сценариях.

**Смотрите также**

-   [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)

