---
slug: /ru/sql-reference/operators/exists
---
# EXISTS {#exists-operator}

Оператор `EXISTS` проверяет, сколько строк содержит результат выполнения подзапроса. Если результат пустой, то оператор возвращает `0`. В остальных случаях оператор возвращает `1`.

`EXISTS` может быть использован в секции [WHERE](../../sql-reference/statements/select/where.md).

:::danger Предупреждение
Ссылки на таблицы или столбцы основного запроса не поддерживаются в подзапросе.
:::

**Синтаксис**

```sql
WHERE EXISTS(subquery)
```

**Пример**

Запрос с подзапросом, возвращающим несколько строк:

``` sql
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

Результат:

``` text
┌─count()─┐
│      10 │
└─────────┘
```

Запрос с подзапросом, возвращающим пустой результат:

``` sql
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

Результат:

``` text
┌─count()─┐
│       0 │
└─────────┘
```
