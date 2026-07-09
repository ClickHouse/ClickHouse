---
description: 'Документация по оператору UNION'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'Оператор UNION'
doc_type: 'reference'
---

Вы можете использовать `UNION`, явно указывая `UNION ALL` или `UNION DISTINCT`.

Если не указать `ALL` или `DISTINCT`, поведение будет зависеть от настройки `union_default_mode`. Разность между `UNION ALL` и `UNION DISTINCT` заключается в том, что `UNION DISTINCT` удаляет дубликаты в результате объединения; это эквивалентно `SELECT DISTINCT` из подзапроса, содержащего `UNION ALL`.

С помощью `UNION` можно объединить результаты любого количества запросов `SELECT`. Пример:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Результирующие столбцы сопоставляются по их индексу (порядку внутри `SELECT`). Если имена столбцов не совпадают, для итогового результата берутся имена из первого запроса.

Для `UNION` выполняется приведение типов. Например, если в двух объединяемых запросах одно и то же поле имеет совместимые типы `Nullable` и не-`Nullable`, то в результирующем `UNION` это поле будет иметь тип `Nullable`.

Запросы, входящие в `UNION`, могут быть заключены в `()`. [ORDER BY](../../../sql-reference/statements/select/order-by.md) и [LIMIT](../../../sql-reference/statements/select/limit.md) применяются к отдельным запросам, а не к итоговому результату. Если вам нужно применить преобразование к итоговому результату, вы можете поместить все запросы с `UNION` в подзапрос в операторе [FROM](../../../sql-reference/statements/select/from.md).

Если вы используете `UNION` без явного указания `UNION ALL` или `UNION DISTINCT`, можно задать режим union с помощью настройки [union&#95;default&#95;mode](/ru/operations/settings/settings#union_default_mode). Значениями настройки могут быть `ALL`, `DISTINCT` или пустая строка. Однако если использовать `UNION`, когда значение настройки `union_default_mode` равно пустой строке, будет сгенерировано исключение. Следующие примеры демонстрируют результаты запросов при разных значениях настройки.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Запросы, входящие в `UNION/UNION ALL/UNION DISTINCT`, могут выполняться одновременно, а их результаты — объединяться вперемешку.

**См. также**

* настройка [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default).
* настройка [union&#95;default&#95;mode](/ru/operations/settings/settings#union_default_mode).