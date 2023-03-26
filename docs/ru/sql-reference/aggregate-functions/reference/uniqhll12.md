---
toc_priority: 194
---

# uniqHLL12 {#agg_function-uniqhll12}

Вычисляет приблизительное число различных значений аргументов, используя алгоритм [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog).

``` sql
uniqHLL12(x[, ...])
```

**Аргументы**

Функция принимает переменное число входных параметров. Параметры могут быть числовых типов, а также `Tuple`, `Array`, `Date`, `DateTime`, `String`.

**Возвращаемое значение**

-   Значение хэша с типом данных [UInt64](../../../sql-reference/data-types/int-uint.md).

**Детали реализации**

Функция:

-   Вычисляет хэш для всех параметров агрегации, а затем использует его в вычислениях.

-   Использует алгоритм HyperLogLog для аппроксимации числа различных значений аргументов.

        Используется 2^12 5-битовых ячеек. Размер состояния чуть больше 2.5 КБ. Результат не точный (ошибка до ~10%) для небольших множеств (<10K элементов). Однако для множеств большой кардинальности (10K - 100M) результат довольно точен (ошибка до ~1.6%). Начиная с 100M ошибка оценки будет только расти и для множеств огромной кардинальности (1B+ элементов) функция возвращает результат с очень большой неточностью.

-   Результат детерминирован (не зависит от порядка выполнения запроса).

Мы не рекомендуем использовать эту функцию. В большинстве случаев используйте функцию [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) или [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined).


-   [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)


