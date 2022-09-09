---
toc_priority: 41
toc_title: ORDER BY
---

# Манипуляции с ключевыми выражениями таблиц {#manipuliatsii-s-kliuchevymi-vyrazheniiami-tablits}

Поддерживается операция:

``` sql
MODIFY ORDER BY new_expression
```

Работает только для таблиц семейства [`MergeTree`](../../../sql-reference/statements/alter/index.md) (в том числе [реплицированных](../../../sql-reference/statements/alter/index.md)). После выполнения запроса
[ключ сортировки](../../../sql-reference/statements/alter/index.md) таблицы
заменяется на `new_expression` (выражение или кортеж выражений). Первичный ключ при этом остаётся прежним.

Операция затрагивает только метаданные. Чтобы сохранить свойство упорядоченности кусков данных по ключу
сортировки, разрешено добавлять в ключ только новые столбцы (т.е. столбцы, добавляемые командой `ADD COLUMN`
в том же запросе `ALTER`), у которых нет выражения по умолчанию.

