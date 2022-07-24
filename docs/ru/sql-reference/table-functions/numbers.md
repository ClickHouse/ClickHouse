---
toc_priority: 39
toc_title: numbers
---

# numbers {#numbers}

`numbers(N)` - возвращает таблицу с единственным столбцом `number` (UInt64), содержащим натуральные числа от `0` до `N-1`.
`numbers(N, M)` - возвращает таблицу с единственным столбцом `number` (UInt64), содержащим натуральные числа от `N` to `(N + M - 1)`.

Так же как и таблица `system.numbers` может использоваться для тестов и генерации последовательных значений. Функция `numbers(N, M)` работает более эффективно, чем выборка из `system.numbers`.

Следующие запросы эквивалентны:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0,10);
SELECT * FROM system.numbers LIMIT 10;
```

Примеры:

``` sql
-- генерация последовательности всех дат от 2010-01-01 до 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

