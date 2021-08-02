---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Инициализирует агрегацию для введеных строчек. Предназначена для функций с суффиксом `State`.
Поможет вам проводить тесты или работать со столбцами типов: `AggregateFunction` и `AggregationgMergeTree`.

**Синтаксис**

``` sql
initializeAggregation (aggregate_function, column_1, column_2);
```

**Параметры**

-   `aggregate_function` — название функции агрегации, состояние которой нужно создать. [String](../../../sql-reference/data-types/string.md#string).
-   `column_n` — столбец, который передается в функцию агрегации как аргумент. [String](../../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

Возвращает результат агрегации введенной информации. Тип возвращаемого значения такой же, как и для функции, которая становится первым аргументом для `initializeAgregation`.

Пример:

Возвращаемый тип функций с суффиксом `State`  — `AggregateFunction`.

**Пример**

Запрос:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
Результат:

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
