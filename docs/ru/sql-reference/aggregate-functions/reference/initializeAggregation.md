---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

Инициализирует агрегацию для строк, которые вы ввели. Придумана для функций с суффиксом `State`.
Пользуйтесь ее для проверок или обработки столбцов типов: `AggregateFunction` и `AggregationgMergeTree`.

**Синтаксис**

``` sql
initializeAggregation (aggregate_function, column_1, column_2);
```

**Параметры**

-   `aggregate_function` — название функции агрегации, состояние которой нужно создать (мы создаем). [String](../../../sql-reference/data-types/string.md#string).
-   `column_n` — столбец, который передается в функцию агрегации в качестве аргумента. [String](../../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

Возвращает результат агрегации того, что вы ввели. Тип возвращаемого значения будет таким же, как тип возвращаемой функции, которая становится первым аргументом для `initializeAgregation`.
К примеру, для функций с суффиксом `State` возвращаемый тип будет `AggregateFunction`.

**Пример**

Запрос:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
Результат:

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
