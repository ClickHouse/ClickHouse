---
toc_title: UNION
---

# Секция UNION {#union-clause}

Вы можете использовать `UNION` в двух режимах: `UNION ALL` или `UNION DISTINCT`.

По умолчанию, `UNION` ведет себя так же, как и `UNION DISTINCT`. Разница между `UNION ALL` и `UNION DISTINCT` в том, что `UNION DISTINCT` выполняет явное преобразование для результата объединения. Это равнозначно выражению `SELECT DISTINCT` из подзапроса, содержащего `UNION ALL`.

Вы можете использовать `UNION ALL` чтобы объединить любое количество `SELECT` запросы путем расширения их результатов. Пример:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Результирующие столбцы сопоставляются по их индексу (порядку внутри `SELECT`). Если имена столбцов не совпадают, то имена для конечного результата берутся из первого запроса.

При объединении выполняет приведение типов. Например, если два запроса имеют одно и то же поле с не-`Nullable` и `Nullable` совместимыми типами, полученные в результате `UNION ALL` данные будут иметь `Nullable` тип.

Запросы, которые являются частью `UNION ALL` не могут быть заключен в круглые скобки. [ORDER BY](order-by.md) и [LIMIT](limit.md) применяются к отдельным запросам, а не к конечному результату. Если вам нужно применить преобразование к конечному результату, вы можете разместить все объединенные с помощью `UNION ALL` запросы в подзапрос в секции [FROM](from.md).

Если используете `UNION` без явного указания `UNION ALL` или `UNION DISTINCT`, то вы можете указать режим объединения с помощью настройки [union_default_mode](../../../operations/settings/settings.md#union-default-mode), значениями которой могут быть `ALL`, `DISTINCT` или пустая строка. Однако если вы используете `UNION` с настройкой `union_default_mode`, значением которой является пустая строка, то будет сгенерировано исключение. В следующих примерах продемонстрированы результаты запросов при разных значениях настройки.

Запрос:

```sql
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

Результат:

```text
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

Запрос:

```sql
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

Результат:

```text
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

Запросы, которые являются частью `UNION/UNION ALL/UNION DISTINCT`, выполняются параллельно, и их результаты могут быть смешаны вместе.

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/select/union/) <!-- hide -->
